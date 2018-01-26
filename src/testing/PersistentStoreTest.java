package testing;

import junit.framework.TestCase;
import org.apache.log4j.BasicConfigurator;
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
    public void testPutGet() throws Exception {
        storeFile.clearStorage();
        String value;

        storeFile.put("hello", "world");
        value = storeFile.get("hello");
        assertTrue(value.equals("world"));

        storeFile.put("wokeshuaile", "noproblem");
        value = storeFile.get("wokeshuaile");
        assertTrue(value.equals("noproblem"));

        storeFile.put("what", "you\rhuo");
        value = storeFile.get("what");
        assertTrue(value.equals("you\rhuo"));

        storeFile.put("你好", "世界");
        value = storeFile.get("你好");
        assertTrue(value.equals("世界"));

        storeFile.put("hello", "null");
        value = storeFile.get("hello");
        assertTrue(value == null);

        storeFile.put("你好", "shijie");
        value = storeFile.get("你好");
        assertTrue(value.equals("shijie"));

        storeFile.put("wocao", "null");
        value = storeFile.get("wocao");
        assertTrue(value == null);
        assertFalse(storeFile.inStorage("wocao"));

        storeFile.put("shei", "bushiwo");
        value = storeFile.get("shei");
        assertTrue(value.equals("bushiwo"));
        assertTrue(storeFile.inStorage("shei"));

        storeFile.put("wokeshuaile", "null");
        value = storeFile.get("wokeshuaile");
        assertTrue(value == null);
        assertTrue(!storeFile.inStorage("wokeshuaile"));

        value = storeFile.get("hello");
        assertTrue(value == null);
        assertTrue(!storeFile.inStorage("hello"));

        value = storeFile.get("what");
        assertTrue(value.equals("you\rhuo"));
        assertTrue(storeFile.inStorage("what"));

        value = storeFile.get("你好");
        assertTrue(value.equals("shijie"));
        assertTrue(storeFile.inStorage("你好"));

        value = storeFile.get("wocao");
        assertTrue(value == null);
        assertTrue(!storeFile.inStorage("wocao"));

        value = storeFile.get("shei");
        assertTrue(value.equals("bushiwo"));
        assertTrue(storeFile.inStorage("shei"));

    }

    @Test
    public void testAfterFirst() {
        String value;
        try {

            value = storeFile.get("hello");
            assertTrue(value == null);
            assertTrue(!storeFile.inStorage("hello"));

            value = storeFile.get("what");
            assertTrue(value.equals("youhuo"));
            assertTrue(storeFile.inStorage("what"));

            value = storeFile.get("你好");
            assertTrue(value.equals("shijie"));
            assertTrue(storeFile.inStorage("你好"));

            value = storeFile.get("wocao");
            assertTrue(value == null);
            assertTrue(!storeFile.inStorage("wocao"));

            value = storeFile.get("shei");
            assertTrue(value.equals("bushiwo"));
            assertTrue(storeFile.inStorage("shei"));


        }
        catch (Exception e){
            ex = e;
        }
        assertNull(ex);

    }



}
