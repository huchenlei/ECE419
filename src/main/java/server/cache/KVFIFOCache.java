package server.cache;

import java.util.LinkedHashMap;
import java.util.Map;

public class KVFIFOCache extends AbstractKVCache {
    public KVFIFOCache(Integer cacheSize) {
        super(cacheSize);
        // overwrite the method to permit removeEldestEntry, the map will automatically remove eldest entry
        this.cacheMap = new LinkedHashMap<String, String>(
                this.getCacheSize(),
                (float) 0.75,
                false
                // accessOrder - the ordering mode - true for access-order, false for insertion-order
        ) {
            @Override
            protected boolean removeEldestEntry(Map.Entry eldest) {
                return size() > getCacheSize();
            }
        };
    }
}
