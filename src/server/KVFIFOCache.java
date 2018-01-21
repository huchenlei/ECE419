package server;

import java.util.LinkedHashMap;
import java.util.Map;

public class KVFIFOCache implements KVCache{
    private int cacheSize = 3; //default value
    private LinkedHashMap<String, String> cacheMap;
    private KVPersistentStore kvStore;

    public KVFIFOCache() {
        // overwrite the method to permit removeEldestEntry, the map will automatically remove eldest entry
        this.cacheMap = new LinkedHashMap<>() {
            @Override
            protected boolean removeEldestEntry(Map.Entry eldest) {
                return size() > cacheSize;
            }
        };
        this.kvStore = new KVIterateStore();
    }

    public void clearStorage() {
        this.kvStore.clearStorage();
    }

    @Override
    public void put(String key, String value) throws Exception {
        boolean isInCache = this.inCache(key);
        boolean isInStorage = isInCache || kvStore.inStorage(key);

        if (value == "null"){
            if (isInStorage) {
                kvStore.put(key,value);
                cacheMap.remove(key);
            }
        }
        else {
            cacheMap.put(key,value);
            kvStore.put(key,value);
        }

    }

    @Override
    public String get(String key) throws Exception {
        String value;
        if (!inCache(key)) {
            value = kvStore.get(key);
            if (value != null){
                cacheMap.put(key, value);
            }
        }
        else {
            value = cacheMap.get(key);
        }
        return value;
    }

    @Override
    public void clearCache() {
        this.cacheMap.clear();
    }

    @Override
    public boolean inCache(String key) {
        return this.cacheMap.containsKey(key);
    }

    @Override
    public int getCacheSize() {
        return this.cacheSize;
    }

    @Override
    public void setCacheSize(int size) {
        this.cacheSize = size;
    }
}
