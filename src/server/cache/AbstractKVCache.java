package server.cache;

import java.util.Map;

public class AbstractKVCache implements KVCache {
    /**
     * Size of max entries the cache can hold
     */
    private int cacheSize;

    /**
     * Map that holds all values
     * Different cache implementations might choose different Map implementations
     */
    protected Map<String, String> cacheMap;

    /**
     * Default constructor
     *
     * @param cacheSize cache size
     */
    public AbstractKVCache(Integer cacheSize) {
        this.cacheSize = cacheSize;
    }

    /**
     * Default implementation of put
     *
     * @param key   key in db
     * @param value value in db
     */
    @Override
    public void put(String key, String value) {
        if ("null".equals(value)) {
            cacheMap.remove(key);
        } else {
            cacheMap.put(key, value);
        }
    }

    /**
     * Default implementation of get
     *
     * @param key key in db
     * @return value
     */
    @Override
    public String get(String key) {
        return cacheMap.get(key);
    }

    @Override
    public void clear() {
        cacheMap.clear();
    }

    @Override
    public int getCacheSize() {
        return this.cacheSize;
    }

    @Override
    public void setCacheSize(int size) {
        this.cacheSize = size;
    }

    @Override
    public boolean containsKey(String key) {
        return cacheMap.containsKey(key);
    }
}
