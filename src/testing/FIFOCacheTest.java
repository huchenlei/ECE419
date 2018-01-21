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
        BasicConfigurator.configure();
        fifoCache = new KVFIFOCache(3);
    }

    @Test
    public void testSimplePutGet() {
        fifoCache.clear();
        fifoCache.setCacheSize(3);

        try{
            assert fifoCache.getCacheSize() == 3;
            assert !fifoCache.containsKey("anything");
            assert fifoCache.get("otherthing") == null;

            fifoCache.put("one","1");

            fifoCache.put("two","2");
            fifoCache.put("three","3");

            assert fifoCache.containsKey("one");
            assert fifoCache.containsKey("two");
            assert fifoCache.containsKey("three");
            assert fifoCache.get("one").equals("1");
            assert fifoCache.get("two").equals("2");
            assert fifoCache.get("three").equals("3");

            fifoCache.put("four","4");
            assert !fifoCache.containsKey("one");
            assert fifoCache.containsKey("two");
            assert fifoCache.containsKey("three");
            assert fifoCache.containsKey("four");

            assert fifoCache.get("one").equals("1");
            assert !fifoCache.containsKey("two");

            fifoCache.put("five","5");
            assert !fifoCache.containsKey("three");
            assert fifoCache.get("one").equals("1");
            assert fifoCache.containsKey("four");
            assert fifoCache.containsKey("one");
            assert fifoCache.containsKey("five");
            assert !fifoCache.containsKey("two");

            fifoCache.put("two", "22");
            assert !fifoCache.containsKey("four");
            assert fifoCache.containsKey("two");
            assert fifoCache.get("two").equals("22");

            assert fifoCache.get("six") == null;
            assert fifoCache.containsKey("one");

            fifoCache.put("three","null");
            assert fifoCache.containsKey("one");
            assert !fifoCache.containsKey("tree");
            assert fifoCache.get("three") == null;
            assert fifoCache.containsKey("one");

            fifoCache.put("five","null");
            assert fifoCache.containsKey("one");
            assert !fifoCache.containsKey("five");
            assert fifoCache.get("five") == null;

            fifoCache.put("six", "6");
            assert fifoCache.containsKey("six");
            assert fifoCache.containsKey("one");

            assert fifoCache.get("four").equals("4");
            assert !fifoCache.containsKey("one");
            

        }
        catch (Exception e) {
            ex = e;
        }

        assertNull(ex);

    }
}
