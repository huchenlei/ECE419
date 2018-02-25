package testing;

import junit.framework.TestCase;
import org.apache.log4j.BasicConfigurator;
import org.junit.Test;
import server.cache.KVFIFOCache;

public class FIFOCacheTest extends TestCase {

    private Exception ex;
    private KVFIFOCache fifoCache;

    @Override
    public void setUp() {
        ex = null;
        fifoCache = new KVFIFOCache(3);
    }

    @Test
    public void testSimplePutGet() {
        fifoCache.clear();
        fifoCache.setCacheSize(3);
        String value = null;
        assertTrue(fifoCache.getCacheSize() == 3);
        assertTrue(!fifoCache.containsKey("anything"));
        assertNull(fifoCache.get("otherthing"));

        fifoCache.put("one", "1");
        fifoCache.put("two", "2");
        fifoCache.put("three", "3");

        assertTrue(fifoCache.containsKey("one"));
        assertTrue(fifoCache.containsKey("two"));
        assertTrue(fifoCache.containsKey("three"));
        value = fifoCache.get("one");
        assertEquals(value, "1");
        fifoCache.put("four", "4");
        assertFalse(fifoCache.containsKey("one"));
    }
}
