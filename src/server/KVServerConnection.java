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
                    logger.error("Connection lost!");
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
        switch (m.getStatus()) {
            case GET: {
                Exception ex = null;
                String value = null;
                try {
                    value = kvServer.getKV(m.getKey());
                } catch (Exception e) {
                    ex = e;

                }
                if (ex != null || value == null)
                    return new SimpleKVMessage(m.getKey(), "", KVMessage.StatusType.GET_ERROR);
                else
                    return new SimpleKVMessage(m.getKey(), value, KVMessage.StatusType.GET_SUCCESS);
            }

            case PUT: {
                boolean keyExist =
                        kvServer.inCache(m.getKey()) || kvServer.inStorage(m.getKey());

                try {
                    kvServer.putKV(m.getKey(), m.getValue());
                } catch (Exception e) {
                    if ("null".equals(m.getValue()))
                        return new SimpleKVMessage(m.getKey(), m.getValue(), KVMessage.StatusType.DELETE_ERROR);
                    else
                        return new SimpleKVMessage(m.getKey(), m.getValue(), KVMessage.StatusType.PUT_ERROR);
                }

                if ("null".equals(m.getValue())) {
                    return new SimpleKVMessage(m.getKey(), m.getValue(), KVMessage.StatusType.DELETE_SUCCESS);
                } else if (keyExist) {
                    return new SimpleKVMessage(m.getKey(), m.getValue(), KVMessage.StatusType.PUT_UPDATE);
                } else {
                    return new SimpleKVMessage(m.getKey(), m.getValue(), KVMessage.StatusType.PUT_SUCCESS);
                }
            }

            default: {
                // Status code un-recognized
                return new SimpleKVMessage("", "", KVMessage.StatusType.BAD_STATUS_ERROR);
            }
        }
    }
}
