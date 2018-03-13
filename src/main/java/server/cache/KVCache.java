package server.cache;

public interface KVCache {
    /**
     * Insert a new key - value pair in cache
     * If cache is full(number of key - value pair exceeds cache size), kick out
     * item with various logic
     *
     * @param key   key in db
     * @param value value in db
     */
    public void put(String key, String value);

    /**
     * Get the value of corresponding key in cache
     *
     * @param key key in db
     * @return null if the key is not found in cache
     */
    public String get(String key);

    /**
     * Clear the cache
     */
    public void clear();

    /**
     * Setter of cache size
     *
     * @return cache size
     */
    public int getCacheSize();

    /**
     * Getter of cache size
     *
     * @param size cache size
     */
    public void setCacheSize(int size);

    public boolean containsKey(String key);
}
