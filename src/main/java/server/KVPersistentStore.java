package server;

public interface KVPersistentStore {

    void put(String key, String value) throws Exception;

    String get(String key) throws Exception;

    void clearStorage();

    boolean inStorage(String key) throws Exception;

    String getfileName();

}
