package server;

public interface KVPersistentStore {
    public void put(String key, String value) throws Exception;
    public String get(String key) throws Exception;

}
