package server;

public interface KVCache {
    public void put(String key, String value) throws Exception;
    public String get(String key) throws Exception;
    public void clearCache();
    public boolean inCache(String key);
    public int getCacheSize();
    public void setCacheSize(int size);
}
