package testing;

import junit.framework.TestCase;
import org.apache.log4j.BasicConfigurator;
import org.junit.Test;
import server.KVFIFOCache;

public class FIFOCacheTest extends TestCase {

    private Exception ex;
    private KVFIFOCache fifoCache;
    @Override
    public void setUp() {
        ex = null;
        BasicConfigurator.configure();
        fifoCache = new KVFIFOCache();
    }

    @Test
    public void testSimplePutGet() {
        fifoCache.clearStorage();
        fifoCache.clearCache();
        fifoCache.setCacheSize(3);

        try{
            assert fifoCache.getCacheSize() == 3;
            assert !fifoCache.inCache("anything");
            assert fifoCache.get("otherthing") == null;

            fifoCache.put("one","1");

            fifoCache.put("two","2");
            fifoCache.put("three","3");

            assert fifoCache.inCache("one");
            assert fifoCache.inCache("two");
            assert fifoCache.inCache("three");
            assert fifoCache.get("one").equals("1");
            assert fifoCache.get("two").equals("2");
            assert fifoCache.get("three").equals("3");

            fifoCache.put("four","4");
            assert !fifoCache.inCache("one");
            assert fifoCache.inCache("two");
            assert fifoCache.inCache("three");
            assert fifoCache.inCache("four");

            assert fifoCache.get("one").equals("1");
            assert !fifoCache.inCache("two");

            fifoCache.put("five","5");
            assert !fifoCache.inCache("three");
            assert fifoCache.get("one").equals("1");
            assert fifoCache.inCache("four");
            assert fifoCache.inCache("one");
            assert fifoCache.inCache("five");
            assert !fifoCache.inCache("two");

            fifoCache.put("two", "22");
            assert !fifoCache.inCache("four");
            assert fifoCache.inCache("two");
            assert fifoCache.get("two").equals("22");

            assert fifoCache.get("six") == null;
            assert fifoCache.inCache("one");

            fifoCache.put("three","null");
            assert fifoCache.inCache("one");
            assert !fifoCache.inCache("tree");
            assert fifoCache.get("three") == null;
            assert fifoCache.inCache("one");

            fifoCache.put("five","null");
            assert fifoCache.inCache("one");
            assert !fifoCache.inCache("five");
            assert fifoCache.get("five") == null;

            fifoCache.put("six", "6");
            assert fifoCache.inCache("six");
            assert fifoCache.inCache("one");

            assert fifoCache.get("four").equals("4");
            assert !fifoCache.inCache("one");
            

        }
        catch (Exception e) {
            ex = e;
        }

        assertNull(ex);

    }
}
