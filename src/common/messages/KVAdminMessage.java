package common.messages;

public class KVAdminMessage implements Encodable, Decodable {
    public enum OperationType {
        INIT, // Initialize the storage service with metadata provided
        START, // Start the storage service
        STOP, // Stops the storage service and only ECS requests are processed
        SHUT_DOWN, // Completely shutdown the server
        LOCK_WRITE, // Lock put operation on server
        UNLOCK_WRITE, // Unlock put operation
        UPDATE, // Update metadata repo of this server
    }


    @Override
    public String encode() {
        return null;
    }

    @Override
    public void decode(String data) {

    }
}
