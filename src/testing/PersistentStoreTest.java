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
            assert !storeFile.inStorage("wocao");

            storeFile.put("shei","bushiwo");
            value = storeFile.get("shei");
            assert(value.equals("bushiwo"));
            assert(storeFile.inStorage("shei"));

            storeFile.put("wokeshuaile","null");
            value = storeFile.get("wokeshuaile");
            assert(value == null);
            assert(!storeFile.inStorage("wokeshuaile"));

            value = storeFile.get("hello");
            assert(value == null);
            assert(!storeFile.inStorage("hello"));

            value = storeFile.get("what");
            assert(value.equals("youhuo"));
            assert(storeFile.inStorage("what"));

            value = storeFile.get("你好");
            assert(value.equals("shijie"));
            assert(storeFile.inStorage("你好"));

            value = storeFile.get("wocao");
            assert(value == null);
            assert(!storeFile.inStorage("wocao"));

            value = storeFile.get("shei");
            assert(value.equals("bushiwo"));
            assert(storeFile.inStorage("shei"));


        } catch (Exception e) {
            ex = e;
        }
        assertNull(ex);

    }

}
