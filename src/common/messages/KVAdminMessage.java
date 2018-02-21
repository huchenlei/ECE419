package common.messages;

import com.google.gson.Gson;

public class KVAdminMessage implements Encodable, Decodable {
    public enum OperationType {
        START, // Start the storage service
        STOP, // Stops the storage service and only ECS requests are processed
        SHUT_DOWN, // Completely shutdown the server
        LOCK_WRITE, // Lock put operation on server
        UNLOCK_WRITE, // Unlock put operation
        UPDATE, // Update metadata repo of this server
    }

    private OperationType operationType;

    public KVAdminMessage(OperationType operationType) {
        this.operationType = operationType;
    }

    @Override
    public String encode() {
        return new Gson().toJson(this);
    }

    @Override
    public void decode(String data) {
        KVAdminMessage msg = new Gson().fromJson(data, this.getClass());
        this.operationType = msg.operationType;
    }
}
