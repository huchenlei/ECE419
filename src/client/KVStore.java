package client;

import common.connection.AbstractKVConnection;
import common.messages.AbstractKVMessage;
import common.messages.KVMessage;
import common.messages.TextMessage;
import ecs.ECSHashRing;

import java.net.Socket;

/**
 * Represents a store session(connection) from client to server.
 */
public class KVStore extends AbstractKVConnection implements KVCommInterface {
    private String address;
    private int port;
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
        this.hashRing = new ECSHashRing();
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
        sendMessage(new TextMessage(req.encode()));
        res.decode(receiveMessage().getMsg());
        //if res is NOT_RESPONSIBLE, the message should include new metadata, retry the request
        if (res.getStatus().equals(KVMessage.StatusType.SERVER_NOT_RESPONSIBLE)) {
        		hashRingString = res.getValue();
        		hashRing = new ECSHashRing(hashRingString);
        		this.address = hashRing.getNodeByKey(key).getNodeHost();
        		this.port = hashRing.getNodeByKey(key).getNodePort();
        		return this.resendRequest(key, value, req.getStatus());
        }
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
        sendMessage(new TextMessage(req.encode()));
        res.decode(receiveMessage().getMsg());
        //if res is NOT_RESPONSIBLE, the message should include new metadata, retry the request
        if (res.getStatus().equals(KVMessage.StatusType.SERVER_NOT_RESPONSIBLE)) {
        		hashRingString = res.getValue();
        		hashRing = new ECSHashRing(hashRingString);
        		this.address = hashRing.getNodeByKey(key).getNodeHost();
        		this.port = hashRing.getNodeByKey(key).getNodePort();
        		return this.resendRequest(key, null, req.getStatus());
        }
        return res;
    }
    
    private KVMessage resendRequest(String key, String value, KVMessage.StatusType request) throws Exception {
 
    		disconnect();
    		connect();
    		if (request.equals(KVMessage.StatusType.PUT)) {
    			return this.put(key, value);
    		}
    		else {
    			return this.get(key);
    		}
    }
}
