package client;

import common.connection.AbstractKVConnection;
import common.messages.AbstractKVMessage;
import common.messages.KVMessage;
import common.messages.TextMessage;
import ecs.ECSHashRing;
import ecs.ECSNode;

import java.net.Socket;

/**
 * Represents a store session(connection) from client to server.
 */
public class KVStore extends AbstractKVConnection implements KVCommInterface {
    private String address;
    private int port;
    private int detect;
    private String hashRingString;
    private ECSHashRing hashRing;

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
        this.detect = 0;
    }

    @Override
    public void disconnect() {
        super.disconnect();
    }

    @Override
    public void connect() throws Exception {
        this.clientSocket = new Socket(address, port);
        this.input = clientSocket.getInputStream();
        this.output = clientSocket.getOutputStream();
    }

    @Override
    public KVMessage put(String key, String value) throws Exception {
        if ("".equals(value)) {
            value = "null";
        }
        KVMessage req = AbstractKVMessage.createMessage();
        KVMessage res = AbstractKVMessage.createMessage();
        assert res != null;
        assert req != null;
        req.setKey(key);
        req.setValue(value);
        req.setStatus(KVMessage.StatusType.PUT);
        //if metadata is not null, find the server responsible for the key
        if (this.detect == 1) {
        		reconnect(req);
        }
        sendMessage(new TextMessage(req.encode()));
        res.decode(receiveMessage().getMsg());
        res = handleNotResponsible(req, res);
        return res;
    }

    @Override
    public KVMessage get(String key) throws Exception {
        KVMessage req = AbstractKVMessage.createMessage();
        KVMessage res = AbstractKVMessage.createMessage();
        assert res != null;
        assert req != null;
        req.setKey(key);
        req.setValue("");
        req.setStatus(KVMessage.StatusType.GET);
        //if metadata is not null, find the server responsible for the key
        if (this.detect == 1) {
    			reconnect(req);
        }
        sendMessage(new TextMessage(req.encode()));
        res.decode(receiveMessage().getMsg());
        res = handleNotResponsible(req, res);
        return res;
    }
    
    private void reconnect(KVMessage req) throws Exception{
    		String hash = ECSNode.calcHash(req.getKey());
    		ECSNode newServer = hashRing.getNodeByKey(hash);
    		String addr = newServer.getNodeHost();
    		int pt = newServer.getNodePort();
    		if (addr.equals(this.address) && pt == this.port) {
    			return;
    		}
    		this.address = addr;
    		this.port = pt;
    		disconnect();
    		connect();
    }

    private KVMessage handleNotResponsible(KVMessage req, KVMessage res) throws Exception {
        if (res.getStatus().equals(KVMessage.StatusType.SERVER_NOT_RESPONSIBLE)) {
        		
            hashRingString = res.getValue();
            hashRing = new ECSHashRing(hashRingString);
            this.detect = 1;
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
            } else if (req.getStatus().equals(KVMessage.StatusType.PUT)){
                return resendRequest(req.getKey(), req.getValue(), req.getStatus());
            } else {
                logger.fatal("Unexpected status type " + req.getStatus());
                return null;
            }
        }
        return res;
    }

    private KVMessage resendRequest(String key, String value, KVMessage.StatusType request) throws Exception {

    		disconnect();
        logger.info("used for debugs");
        logger.info(this.address);
        logger.info(this.port);
        connect();
        if (request.equals(KVMessage.StatusType.PUT)) {
            return this.put(key, value);
        } else {
            return this.get(key);
        }
    }
}
