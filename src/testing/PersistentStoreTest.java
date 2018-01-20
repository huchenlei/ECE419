package testing;

import junit.framework.TestCase;

import org.apache.log4j.BasicConfigurator;
import org.junit.Before;
import org.junit.Test;
import server.KVIterateStore;



public class PersistentStoreTest extends TestCase {
    private KVIterateStore storeFile;
    private Exception ex;

    @Override
    public void setUp() {
        BasicConfigurator.configure();
        ex = null;
        storeFile = new KVIterateStore();
    }


    @Test
    public void testPutGet() {
        storeFile.clearStorage();
        String value = "initial";
        try {
            storeFile.put("hello","world");
            value = storeFile.get("hello");
            assert(value.equals("world"));

            storeFile.put("wokeshuaile","noproblem");
            value = storeFile.get("wokeshuaile");
            assert(value.equals("noproblem"));

            storeFile.put("what","youhuo");
            value = storeFile.get("what");
            assert(value.equals("youhuo"));

            storeFile.put("你好","世界");
            value = storeFile.get("你好");
            assert(value.equals("世界"));

            storeFile.put("hello","null");
            value = storeFile.get("hello");
            assert(value == null);

            storeFile.put("你好","shijie");
            value = storeFile.get("你好");
            assert(value.equals("shijie"));

            storeFile.put("wocao","null");
            value = storeFile.get("wocao");
            assert(value == null);

            storeFile.put("shei","bushiwo");
            value = storeFile.get("shei");
            assert(value.equals("bushiwo"));

            storeFile.put("wokeshuaile","null");
            value = storeFile.get("wokeshuaile");
            assert(value == null);

            value = storeFile.get("hello");
            assert(value == null);

            value = storeFile.get("what");
            assert(value.equals("youhuo"));

            value = storeFile.get("你好");
            assert(value.equals("shijie"));

            value = storeFile.get("wocao");
            assert(value == null);

            value = storeFile.get("shei");
            assert(value.equals("bushiwo"));


        } catch (Exception e) {
            ex = e;
        }
        assertNull(ex);

    }

}
