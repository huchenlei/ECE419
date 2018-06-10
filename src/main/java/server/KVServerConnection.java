package server;

import app_kvServer.IKVServer;
import app_kvServer.KVServer;
import com.google.gson.Gson;
import common.connection.AbstractKVConnection;
import common.messages.AbstractKVMessage;
import common.messages.KVMessage;
import common.messages.SQLJoinMessage;
import common.messages.TextMessage;
import ecs.ECSHashRing;
import ecs.ECSNode;
import server.sql.SQLException;
import server.sql.SQLExecutor;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.net.Socket;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
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
    private SQLExecutor executor;

    public KVServerConnection(KVServer kvServer, Socket clientSocket) {
        this.kvServer = kvServer;
        this.clientSocket = clientSocket;
        this.open = true;
        this.setPrompt(kvServer.getServerName());
        this.forwarderManager = kvServer.getForwarderManager();
        this.executor = new SQLExecutor(kvServer.getSqlStore(), kvServer.getQuerent());
    }


    @Override
    public void run() {
        try {
            this.input = new BufferedInputStream(clientSocket.getInputStream());
            this.output = new BufferedOutputStream(clientSocket.getOutputStream());

            while (isOpen()) {
                try {
                    AbstractKVMessage req = AbstractKVMessage.createMessage();
                    assert req != null;
                    req.decode(receiveMessage().getMsg());
                    if (!kvServer.isRunning()) {
                        logger.info(kvServer.prompt() + "Server not running");
                        disconnect();
                        return;
                    }

                    AbstractKVMessage res = handleMsg(req);
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
            kvServer.conns.remove(this);
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
        List<KVMessage.StatusType> allowedTypes = Arrays.asList(
                KVMessage.StatusType.GET,
                KVMessage.StatusType.PUT_REPLICATE,
                KVMessage.StatusType.SQL_REPLICATE,
                KVMessage.StatusType.SQL_JOIN);

        if (allowedTypes.contains(m.getStatus())) {
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
    private AbstractKVMessage handleMsg(AbstractKVMessage m) {
        AbstractKVMessage res = AbstractKVMessage.createMessage();
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

            case SQL_REPLICATE:
            case SQL: {
                res.setStatus(KVMessage.StatusType.SQL_SUCCESS);
                try {
                    String result = executor.executeSQL(m.getValue(),
                            m.getStatus() == KVMessage.StatusType.SQL_REPLICATE);
                    res.setValue(result);
                } catch (IOException | SQLException e) {
                    res.setStatus(KVMessage.StatusType.SQL_ERROR);
                    res.setValue(e.getMessage());
                }

                try {
                    if (m.getStatus() == KVMessage.StatusType.SQL) {
                        forwarderManager.forward(m);
                    }
                } catch (IOException | KVServerForwarder.ForwardFailedException e) {
                    logger.warn("Failed to replicate SQL command!");
                    logger.warn(e.getMessage());
                    e.printStackTrace();
                    res.setStatus(KVMessage.StatusType.SQL_ERROR);
                }
                break;
            }
            case SQL_JOIN:
                res.setStatus(KVMessage.StatusType.SQL_JOIN_SUCCESS);
                try {
                    SQLJoinMessage joinMessage = new Gson().fromJson(m.getValue(), SQLJoinMessage.class);
                    Map<String, Map<String, Object>> resMap = executor.joinSearch(joinMessage.getTableName(),
                            joinMessage.getJoinColName(), joinMessage.getVals(), joinMessage.getSelector());

                    String result = new Gson().toJson(resMap);
                    res.setValue(result);
                } catch (IOException | SQLException e) {
                    logger.warn("Failed to complete SQL join command!");
                    logger.warn(e.getMessage());
                    e.printStackTrace();
                    res.setStatus(KVMessage.StatusType.SQL_ERROR);
                }

                break;
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
