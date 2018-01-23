package server.cache;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * KVCache that use the LRU(Least Recent Use) logic to remove keys when cache is full
 */
public class KVLRUCache extends AbstractKVCache{
    public KVLRUCache(Integer cacheSize) {
        super(cacheSize);
        this.cacheMap = new LinkedHashMap<String, String>(
                this.getCacheSize(),
                (float) 0.75,
                true
                // accessOrder - the ordering mode - true for access-order, false for insertion-order
        ) {
            @Override
            protected boolean removeEldestEntry(Map.Entry eldest) {
                return size() > getCacheSize();
            }
        };
    }
}
