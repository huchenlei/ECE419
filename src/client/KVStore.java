package client;

import common.connection.AbstractKVConnection;
import common.messages.KVMessage;
import common.messages.SimpleKVMessage;
import common.messages.TextMessage;

import java.net.Socket;

/**
 * Represents a store session(connection) from client to server.
 */
public class KVStore extends AbstractKVConnection implements KVCommInterface {
    private String address;
    private int port;

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
    }

    @Override
    public void connect() throws Exception {
        this.clientSocket = new Socket(address, port);
        this.input = clientSocket.getInputStream();
        this.output = clientSocket.getOutputStream();
    }

    @Override
    public KVMessage put(String key, String value) throws Exception {
        sendMessage(
                new TextMessage(
                        new SimpleKVMessage(key, value, KVMessage.StatusType.PUT).encode()));
        return new SimpleKVMessage(receiveMessage().getMsg());
    }

    @Override
    public KVMessage get(String key) throws Exception {
        sendMessage(
                new TextMessage(
                        new SimpleKVMessage(key, "", KVMessage.StatusType.GET).encode()));
        return new SimpleKVMessage(receiveMessage().getMsg());
    }
}
