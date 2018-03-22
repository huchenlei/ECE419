package server;

import app_kvServer.IKVServer;
import app_kvServer.KVServer;
import common.KVMessage;
import common.connection.AbstractKVConnection;
import common.messages.AbstractKVMessage;
import common.messages.TextMessage;
import ecs.ECSHashRing;
import ecs.ECSNode;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.net.Socket;
import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;

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
    private KVServer kvServer;
    private KVServerForwarderManager forwarderManager;

    public KVServerConnection(KVServer kvServer, Socket clientSocket) {
        this.kvServer = kvServer;
        this.clientSocket = clientSocket;
        this.open = true;
        this.setPrompt(kvServer.getServerName());
        this.forwarderManager = kvServer.getForwarderManager();
    }

    @Override
    public void run() {
        try {
            this.input = new BufferedInputStream(clientSocket.getInputStream());
            this.output = new BufferedOutputStream(clientSocket.getOutputStream());

            while (isOpen()) {
                try {
                    KVMessage req = AbstractKVMessage.createMessage();
                    assert req != null;
                    req.decode(receiveMessage().getMsg());
                    if (!kvServer.isRunning()) {
                        disconnect();
                        return;
                    }
                    KVMessage res = handleMsg(req);
                    sendMessage(new TextMessage(res.encode()));
                } catch (IOException ioe) {
                    logger.warn("Connection lost (" + this.clientSocket.getInetAddress().getHostName()
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


    private boolean isResponsible(KVMessage m) {
        ECSHashRing hashRing = kvServer.getHashRing();


        ECSNode node = hashRing.getNodeByKey(ECSNode.calcHash(m.getKey()));
        if (node == null) {
            logger.error("HashRing: " + hashRing);
        }

        assert node != null;

        String serverName = kvServer.getServerName();
        Boolean responsible = node.getNodeName().equals(serverName);
        if (m.getStatus().equals(KVMessage.StatusType.GET)
                || m.getStatus().equals(KVMessage.StatusType.PUT_REPLICATE)) {
            Collection<ECSNode> replicationNodes =
                    hashRing.getReplicationNodes(node);
            responsible = responsible || replicationNodes.stream()
                    .map(ECSNode::getNodeName)
                    .collect(Collectors.toList())
                    .contains(serverName);

        }
        return responsible;
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

        if (kvServer.isDistributed()) {
            // stopped server can not handle any request
            if (kvServer.getStatus().equals(IKVServer.ServerStatus.STOP)) {
                res.setValue("");
                res.setStatus(KVMessage.StatusType.SERVER_STOPPED);
                return res;
            }
            if (!isResponsible(m)) {
                res.setValue(kvServer.getHashRingString());
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
                        res.setValue(ex.getMessage() + ": " + Arrays.toString(ex.getStackTrace()));
                    else
                        res.setValue("Key(" +
                                new BigInteger(1, m.getKey().getBytes()).toString(16)
                                + ")(hash: " + ECSNode.calcHash(m.getKey())
                                + ") not found on server " + kvServer.getServerName());

                    res.setStatus(KVMessage.StatusType.GET_ERROR);
                } else {
                    res.setValue(value);
                    res.setStatus(KVMessage.StatusType.GET_SUCCESS);
                }
                break;
            }

            case PUT_REPLICATE:
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
                    // Forward the message if its coordinator
                    if (KVMessage.StatusType.PUT.equals(m.getStatus())) {
                        forwarderManager.forward(m);
                    }
                } catch (Exception e) {
                    if ("null".equals(m.getValue()))
                        res.setStatus(KVMessage.StatusType.DELETE_ERROR);
                    else {
                        logger.warn("Failed to put kv " + e.getMessage());
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
