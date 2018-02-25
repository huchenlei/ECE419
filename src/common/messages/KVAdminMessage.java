package common.messages;

import com.google.gson.Gson;

import java.util.Arrays;

public class KVAdminMessage implements Encodable, Decodable {
    public enum OperationType {
        INIT, // Used by awaitNodes
        START, // Start the storage service
        STOP, // Stops the storage service and only ECS requests are processed
        SHUT_DOWN, // Completely shutdown the server
        LOCK_WRITE, // Lock put operation on server
        UNLOCK_WRITE, // Unlock put operation
        UPDATE, // Update metadata repo of this server
        RECEIVE, // Open a socket for transferring data
        SEND, // Send data to target host and port in certain hash range
    }

    private OperationType operationType;

    public KVAdminMessage(OperationType operationType) {
        this.operationType = operationType;
    }

    private String receiverName = null;
    private String receiverHost = null;
    private String[] hashRange = null;


    @Override
    public String encode() {
        return new Gson().toJson(this);
    }

    @Override
    public void decode(String data) {
        KVAdminMessage msg = new Gson().fromJson(data, this.getClass());
        this.operationType = msg.operationType;
        this.receiverName = msg.receiverName;
        this.hashRange = msg.hashRange;
        this.receiverHost = msg.receiverHost;
    }

    public OperationType getOperationType() {
        return operationType;
    }

    public String getReceiverName() {
        return receiverName;
    }

    public String getReceiverHost() {
        return receiverHost;
    }

    public String[] getHashRange() {
        return hashRange;
    }

    public void setReceiverName(String receiverName) {
        this.receiverName = receiverName;
    }

    public void setReceiverHost(String receiverHost) {
        this.receiverHost = receiverHost;
    }

    public void setHashRange(String[] hashRange) {
        this.hashRange = hashRange;
    }

    @Override
    public String toString() {
        return "KVAdminMessage{" +
                "operationType=" + operationType +
                ", receiverName='" + receiverName + '\'' +
                ", receiverHost='" + receiverHost + '\'' +
                ", hashRange=" + Arrays.toString(hashRange) +
                '}';
    }
}
