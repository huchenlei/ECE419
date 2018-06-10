package client;

import common.connection.AbstractKVConnection;
import common.messages.AbstractKVMessage;
import common.messages.KVMessage;
import common.messages.TextMessage;
import ecs.ECSHashRing;
import ecs.ECSNode;
import server.sql.SQLParser;
import server.sql.SQLScanner;

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
        AbstractKVMessage res = AbstractKVMessage.createMessage();
        assert res != null;
        try {
            //if metadata is not null, find the server responsible for the key
            dispatchToCorrectServer(req);
            sendMessage(new TextMessage(((AbstractKVMessage) req).encode()));
            res.decode(receiveMessage().getMsg());
            res = handleNotResponsible((AbstractKVMessage) req, res);
        } catch (IOException e) {
            logger.warn(e.getMessage());
            res = (AbstractKVMessage) handleShutdown((AbstractKVMessage) req);
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

    public KVMessage sql(String sqlString) throws IOException {
        KVMessage req = AbstractKVMessage.createMessage();
        assert req != null;
        req.setStatus(KVMessage.StatusType.SQL);
        String table = SQLParser.parse(SQLScanner.scan(sqlString)).table;
        req.setKey(ECSNode.calcHash(table));
        req.setValue(sqlString);
        return request(req);
    }


    private KVMessage handleShutdown(AbstractKVMessage req) {
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
                return dispatchRequest(req);
            }
            return null;
        } catch (IOException e) {
            logger.warn(e.getMessage());
            return handleShutdown(req);
        }
    }

    private AbstractKVMessage dispatchRequest(AbstractKVMessage req) throws IOException {
        switch (req.getStatus()) {
            case PUT:
                return (AbstractKVMessage) this.put(req.getKey(), req.getValue());
            case GET:
                return (AbstractKVMessage) this.get(req.getKey());
            case SQL:
                return (AbstractKVMessage) this.sql(req.getValue());
            default:
                return null;
        }
    }

    private void dispatchToCorrectServer(KVMessage req) throws IOException {
        if (hashRing == null) return;
        assert req != null;
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

    private AbstractKVMessage handleNotResponsible(AbstractKVMessage req, AbstractKVMessage res) throws IOException {
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
            disconnect();
            connect();
            return dispatchRequest(req);
        }
        return res;
    }
}
