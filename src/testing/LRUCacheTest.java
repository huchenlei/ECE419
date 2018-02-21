package testing;

import junit.framework.TestCase;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Test;
import server.cache.KVLRUCache;

public class LRUCacheTest extends TestCase{
    private static final Integer CACHE_SIZE = 128;
    private KVLRUCache cache;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        cache = new KVLRUCache(CACHE_SIZE);
}

    /**
     * This test test the cache's ability to remove least recently used item
     * from cache when cache is full
     */
    @Test
    public void testSimpleLRU() {
        // fill in the cache
        for (int i = 0; i < CACHE_SIZE; i++) {
            cache.put("key" + i, "val" + i);
        }

        // Touch the first item in cache
        cache.get("key0");

        // cache is full now, will remove one item from cache
        // because the first item is just touched
        // the second item will be LRU, and thus need to be removed
        cache.put("keyAdditional", "val");

        assertNotNull(cache.get("key0"));
        assertNull(cache.get("key1"));
    }
}
