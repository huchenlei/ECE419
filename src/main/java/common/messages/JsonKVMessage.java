package common.messages;

import com.google.gson.Gson;

/**
 * This class is an Json implementation of message exchanging data between
 * server and client
 */
public class JsonKVMessage extends AbstractKVMessage {
    // TODO to be removed after tester bug is fixed by TA
    @Override
    public String getKey() {
        return key;
    }

    @Override
    public String getValue() {
        return value;
    }

    @Override
    public StatusType getStatus() {
        return status;
    }

    @Override
    public String encode() {
        return new Gson().toJson(this);
    }

    @Override
    public void decode(String data) {
        JsonKVMessage json = new Gson().fromJson(data, this.getClass());
        this.key = json.key;
        this.value = json.value;
        this.status = json.status;
    }

    public JsonKVMessage(String key, String value, String status) {
        super(key, value, status);
    }

    public JsonKVMessage() {
    }

    @Override
    public String toString() {
        return "JsonKVMessage{" +
                "key='" + key + '\'' +
                ", status=" + status +
                ", value='" + value + '\'' +
                '}';
    }
}
