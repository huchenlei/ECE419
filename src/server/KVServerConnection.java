package server;

import app_kvServer.IKVServer;
import common.connection.AbstractKVConnection;
import common.messages.KVMessage;
import common.messages.SimpleKVMessage;
import common.messages.TextMessage;

import java.io.IOException;
import java.net.Socket;

/**
 * Represents a connection end point for a particular client that is
 * connected to the server. This class is responsible for message reception
 * and sending on the server side
 * <p>
 * After parsing the message, and getting its contents, will call KVServer to
 * perform corresponding actions.
 * <p>
 * Created by Charlie on 2018-01-12.
 */
public class KVServerConnection extends AbstractKVConnection implements Runnable {
    private IKVServer kvServer;

    public KVServerConnection(IKVServer kvServer, Socket clientSocket) {
        this.kvServer = kvServer;
        this.clientSocket = clientSocket;
        this.open = true;
    }

    @Override
    public void run() {
        try {
            output = clientSocket.getOutputStream();
            input = clientSocket.getInputStream();

            while (isOpen()) {
                try {
                    TextMessage latestMsg = receiveMessage();
                    KVMessage kvMessage = handleMsg(new SimpleKVMessage(latestMsg.getMsg()));
                    sendMessage(new TextMessage(kvMessage.encode()));
                } catch (IOException ioe) {
                    logger.error("Connection lost (" + this.clientSocket.getInetAddress().getHostName()
                            +  ": " + this.clientSocket.getPort() + ")!");
                    this.open = false;
                }
            }
        } catch (IOException e) {
            logger.error("Connection could not be established!", e);
        } finally {
            disconnect();
        }
    }

    /**
     * Parse the message string and dispatch to server action
     *
     * @param m message object
     * @return response string to client
     */
    private KVMessage handleMsg(KVMessage m) {
        KVMessage res = new SimpleKVMessage();
        res.setKey(m.getKey());
        switch (m.getStatus()) {
            case GET: {
                Exception ex = null;
                String value = null;
                try {
                    value = kvServer.getKV(m.getKey());
                } catch (Exception e) {
                    ex = e;

                }
                if (ex != null || value == null) {
                    res.setValue("");
                    res.setStatus(KVMessage.StatusType.GET_ERROR);
                } else {
                    res.setValue(value);
                    res.setStatus(KVMessage.StatusType.GET_SUCCESS);
                }
                break;
            }

            case PUT: {
                res.setKey(m.getKey());
                res.setValue(m.getValue());

                boolean keyExist =
                        kvServer.inCache(m.getKey()) || kvServer.inStorage(m.getKey());

                if ("".equals(m.getKey()) ||
                        "".equals(m.getValue()) ||
                    // Empty string is not allowed as key or value on server side
                    // Value of empty string is supposed to be converted to "null" at the client side
                        m.getKey().matches(".*\\s.*") ||
                        m.getValue().matches(".*\\s.*")
                    // The key or value can not contain any white space characters such as '\t', ' ' or '\n'
                        ) {
                    res.setStatus(KVMessage.StatusType.PUT_ERROR);
                    break;
                }

                try {
                    kvServer.putKV(m.getKey(), m.getValue());
                } catch (Exception e) {
                    if ("null".equals(m.getValue()))
                        res.setStatus(KVMessage.StatusType.DELETE_ERROR);
                    else
                        res.setStatus(KVMessage.StatusType.PUT_ERROR);
                    break;
                }

                if ("null".equals(m.getValue())) {
                    res.setStatus(KVMessage.StatusType.DELETE_SUCCESS);
                } else if (keyExist) {
                    res.setStatus(KVMessage.StatusType.PUT_UPDATE);
                } else {
                    res.setStatus(KVMessage.StatusType.PUT_SUCCESS);
                }
                break;
            }

            default: {
                // Status code un-recognized
                res.setKey("");
                res.setValue("");
                res.setStatus(KVMessage.StatusType.BAD_STATUS_ERROR);
            }
        }
        return res;
    }
}
