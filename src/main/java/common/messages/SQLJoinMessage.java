package common.messages;

import com.google.gson.Gson;

import java.util.List;

public class SQLJoinMessage implements Encodable, Decodable {
    private String tableName;
    private String joinColName;
    private List<Object> vals;
    private List<String> selector;

    public SQLJoinMessage(String tableName, String joinColName, List<Object> vals, List<String> selector) {
        this.tableName = tableName;
        this.joinColName = joinColName;
        this.vals = vals;
        this.selector = selector;
    }

    public String getTableName() {
        return tableName;
    }

    public String getJoinColName() {
        return joinColName;
    }

    public List<Object> getVals() {
        return vals;
    }

    public List<String> getSelector() {
        return selector;
    }

    @Override
    public String encode() {
        return new Gson().toJson(this);
    }

    @Override
    public void decode(String data) {
        SQLJoinMessage msg = new Gson().fromJson(data, this.getClass());
        this.tableName = msg.tableName;
        this.joinColName = msg.joinColName;
        this.vals = msg.vals;
        this.selector = msg.selector;
    }
}
