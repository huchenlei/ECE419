package server;

import app_kvServer.IKVServer;
import app_kvServer.KVServer;
import common.connection.AbstractKVConnection;
import common.messages.AbstractKVMessage;
import common.messages.KVMessage;
import common.messages.TextMessage;
import ecs.ECSHashRing;
import ecs.ECSNode;

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
                    KVMessage req = AbstractKVMessage.createMessage();
                    assert req != null;
                    req.decode(receiveMessage().getMsg());
                    KVMessage res = handleMsg(req);
                    sendMessage(new TextMessage(res.encode()));
                } catch (IOException ioe) {
                    logger.error("Connection lost (" + this.clientSocket.getInetAddress().getHostName()
                            + ": " + this.clientSocket.getPort() + ")!");
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
        KVMessage res = AbstractKVMessage.createMessage();
        assert res != null;
        res.setKey(m.getKey());

        if (((KVServer) kvServer).isDistributed()) {
            ECSHashRing hashRing = ((KVServer) kvServer).getHashRing();
            // stopped server can not handle any request
            if (kvServer.getStatus().equals(IKVServer.ServerStatus.STOP)
                    || hashRing.empty()) {
                res.setValue("");
                res.setStatus(KVMessage.StatusType.SERVER_STOPPED);
                return res;
            }


            ECSNode node = hashRing.getNodeByKey(ECSNode.calcHash(m.getKey()));
            if (node == null) {
                logger.error("HashRing: " + hashRing);
            }

            assert node != null;
            if (!node.getNodeName().equals(((KVServer) kvServer).getServerName())) {
                res.setValue(((KVServer) kvServer).getHashRingString());
                res.setStatus(KVMessage.StatusType.SERVER_NOT_RESPONSIBLE);
                return res;
            }

        }



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
                    if (ex != null)
                        res.setValue(ex.getMessage());
                    else
                        res.setValue("Key not found on server " + ((KVServer) kvServer).getServerName());

                    res.setStatus(KVMessage.StatusType.GET_ERROR);
                } else {
                    res.setValue(value);
                    res.setStatus(KVMessage.StatusType.GET_SUCCESS);
                }
                break;
            }

            case PUT: {

                // if server locked, it can not handle put request
                if (kvServer.getStatus().equals(IKVServer.ServerStatus.LOCK)) {
                    res.setValue("");
                    res.setStatus(KVMessage.StatusType.SERVER_WRITE_LOCK);
                    return res;
                }
                res.setKey(m.getKey());
                res.setValue(m.getValue());

                boolean keyExist =
                        kvServer.inCache(m.getKey()) || kvServer.inStorage(m.getKey());

                if ("".equals(m.getKey()) ||
                        "".equals(m.getValue()) ||
                        // Empty string is not allowed as key or value on server side
                        // Value of empty string is supposed to be converted to "null" at the client side
//                        m.getKey().matches(".*\\s.*") ||
//                        m.getValue().matches(".*\\s.*") ||
                        // The key or value can not contain any white space characters such as '\t', ' ' or '\n'
                        m.getKey().length() > KVServer.MAX_KEY ||
                        m.getValue().length() > KVServer.MAX_VAL
                    // The key or value can not exceed designated length
                        ) {
                    logger.info("Bad key val pair received " + m);
                    res.setStatus(KVMessage.StatusType.PUT_ERROR);
                    break;
                }

                try {
                    kvServer.putKV(m.getKey(), m.getValue());
                } catch (Exception e) {
                    if ("null".equals(m.getValue()))
                        res.setStatus(KVMessage.StatusType.DELETE_ERROR);
                    else {
                        logger.info("Failed to put kv " + e.getMessage());
                        res.setStatus(KVMessage.StatusType.PUT_ERROR);
                    }
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
