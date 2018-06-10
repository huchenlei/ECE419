package server.sql;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import common.connection.AbstractKVConnection;
import common.messages.AbstractKVMessage;
import common.messages.KVMessage;
import common.messages.SQLJoinMessage;
import common.messages.TextMessage;
import ecs.ECSHashRing;
import ecs.ECSNode;

import java.lang.reflect.Type;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SQLJoinQuerent extends AbstractKVConnection{
    private ECSHashRing hashRing;
    private String serverName;

    public SQLJoinQuerent(String serverName, ECSHashRing hashRing) {
        this.hashRing = hashRing;
        this.serverName = serverName;
    }

    public void updateHashRing(ECSHashRing hashRing) {
        this.hashRing = hashRing;
    }

    public Map<String, Map<String, Object>> queryJoin(String tableName, String joinColName,
                                                      List<Object> vals, List<String> selector) throws Exception{
        AbstractKVMessage req = AbstractKVMessage.createMessage();
        AbstractKVMessage res = AbstractKVMessage.createMessage();

        req.setKey(ECSNode.calcHash(tableName));
        if (hashRing == null) {
            throw  new SQLException("JoinQuerent's hashRing is null");
        }
        String hash = ECSNode.calcHash(req.getKey());
        ECSNode node = hashRing.getNodeByKey(hash);

        String addr = node.getNodeHost();
        int pt = node.getNodePort();

        this.address = addr;
        this.port = pt;

        connect();
        SQLJoinMessage joinQuery = new SQLJoinMessage(tableName, joinColName, vals, selector);
        String encodeVal = joinQuery.encode();
        req.setValue(encodeVal);
        req.setStatus(KVMessage.StatusType.SQL_JOIN);

        sendMessage(new TextMessage(req.encode()));
        res.decode(receiveMessage().getMsg());

        // do something
        disconnect();

        if (res.getStatus().equals(KVMessage.StatusType.SQL_ERROR)){
            logger.error(serverName + ": Fail to receive JOIN response");
        }
        Type type = new TypeToken<Map<String, Map<String, Object>>>(){}.getType();
        Map<String, Map<String, Object>> result = new Gson().fromJson(new String(res.getValue()), type);

        return result;
    }

    public boolean isResponsible(String tableName) {
        String key = ECSNode.calcHash(tableName);
        String hash = ECSNode.calcHash(key);
        ECSNode node = hashRing.getNodeByKey(hash);
        Boolean responsible = node.getNodeName().equals(this.serverName);
        Collection<ECSNode> replicationNodes =
                hashRing.getReplicationNodes(node);
        responsible = responsible || replicationNodes.stream()
                .map(ECSNode::getNodeName)
                .collect(Collectors.toList())
                .contains(serverName);

        return responsible;
    }

}
