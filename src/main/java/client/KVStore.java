package client;

import common.KVMessage;
import common.connection.AbstractKVConnection;
import common.messages.AbstractKVMessage;
import common.messages.TextMessage;
import ecs.ECSHashRing;
import ecs.ECSNode;

import java.io.IOException;
import java.util.Collection;

/**
 * Represents a store session(connection) from client to server.
 */
public class KVStore extends AbstractKVConnection implements KVCommInterface {
    private ECSHashRing hashRing;
    private static final String PROMPT = "> ";

    /**
     * Initialize KVStore with address and port of KVServer
     *
     * @param address the address of the KVServer
     * @param port    the port of the KVServer
     */
    public KVStore(String address, int port) {
        this.address = address;
        this.port = port;
        this.open = true;
        hashRing = new ECSHashRing();
        ECSNode node = new ECSNode("node1", this.address, this.port);
        hashRing.addNode(node);
    }

    private KVMessage request(KVMessage req) throws IOException {
        KVMessage res = AbstractKVMessage.createMessage();
        assert res != null;
        try {
            //if metadata is not null, find the server responsible for the key
            dispatchToCorrectServer(req);
            sendMessage(new TextMessage(req.encode()));
            res.decode(receiveMessage().getMsg());
            res = handleNotResponsible(req, res);
        } catch (IOException e) {
            logger.warn(e.getMessage());
            res = handleShutdown(req);
            if (res == null) {
                System.out.println(PROMPT + "Error! " + "All Servers Not In Service");
                disconnect();
                throw e;
            }
        }
        return res;
    }

    @Override
    public KVMessage put(String key, String value) throws IOException {
        if ("".equals(value)) {
            value = "null";
        }
        KVMessage req = AbstractKVMessage.createMessage();
        assert req != null;
        req.setKey(key);
        req.setValue(value);
        req.setStatus(KVMessage.StatusType.PUT);
        return request(req);
    }

    @Override
    public KVMessage get(String key) throws IOException {
        KVMessage req = AbstractKVMessage.createMessage();
        assert req != null;
        req.setKey(key);
        req.setValue("");
        req.setStatus(KVMessage.StatusType.GET);
        return request(req);
    }

    private KVMessage handleShutdown(KVMessage req) {
        disconnect();
        String hash = ECSNode.calcHash(req.getKey());
        if (hashRing.empty()) {
            return null;
        }
        ECSNode toRemove = hashRing.getNodeByKey(hash);
        logger.info("Now remove the node " + toRemove.getNodeName());
        hashRing.removeNode(toRemove);
        ECSNode newServer = hashRing.getNodeByKey(hash);
        try {
            if (newServer != null) {
                this.address = newServer.getNodeHost();
                this.port = newServer.getNodePort();
                logger.info("Now connect to " + this.address + ":" + this.port);
                connect();
                if (req.getStatus().equals(KVMessage.StatusType.PUT)) {
                    return this.put(req.getKey(), req.getValue());
                }
                return this.get(req.getKey());
            }
            return null;
        } catch (IOException e) {
            return handleShutdown(req);
        }
    }

    private void dispatchToCorrectServer(KVMessage req) throws IOException {
        if (hashRing == null) return;
        String hash = ECSNode.calcHash(req.getKey());
        ECSNode node = hashRing.getNodeByKey(hash);

        if (req.getStatus().equals(KVMessage.StatusType.GET)) {
            // Can happen to replica server
            Collection<ECSNode> replicas = hashRing.getReplicationNodes(node);
            if (replicas.contains(new ECSNode("DUMB", this.address, this.port))) {
                return;
            }
        }

        String addr = node.getNodeHost();
        int pt = node.getNodePort();
        if (addr.equals(this.address) && pt == this.port) {
            return;
        }
        this.address = addr;
        this.port = pt;
        disconnect();
        connect();
    }

    private KVMessage handleNotResponsible(KVMessage req, KVMessage res) throws IOException {
        if (res.getStatus().equals(KVMessage.StatusType.SERVER_NOT_RESPONSIBLE)) {
            String hashRingString = res.getValue();
            hashRing = new ECSHashRing(hashRingString);
            String hash = ECSNode.calcHash(res.getKey());
            ECSNode newServer = hashRing.getNodeByKey(hash);

            logger.info("Now connect to " + newServer);
            logger.debug("Node hash range " + newServer.getNodeHashRange()[0]
                    + " -> " + newServer.getNodeHashRange()[1]);
            logger.debug("Key hash is " + hash);

            this.address = newServer.getNodeHost();
            this.port = newServer.getNodePort();
            if (req.getStatus().equals(KVMessage.StatusType.GET)) {
                return resendRequest(req.getKey(), null, req.getStatus());
            } else if (req.getStatus().equals(KVMessage.StatusType.PUT)) {
                return resendRequest(req.getKey(), req.getValue(), req.getStatus());
            } else {
                logger.fatal("Unexpected status type " + req.getStatus());
                return null;
            }
        }
        return res;
    }

    private KVMessage resendRequest(String key, String value, KVMessage.StatusType request) throws IOException {
        disconnect();
        connect();
        if (request.equals(KVMessage.StatusType.PUT)) {
            return this.put(key, value);
        } else {
            return this.get(key);
        }
    }
}
