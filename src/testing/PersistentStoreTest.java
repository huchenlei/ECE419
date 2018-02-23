package testing;

import junit.framework.TestCase;
import logger.LogSetup;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.junit.Test;
import server.KVIterateStore;

import java.io.IOException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class PersistentStoreTest extends TestCase {
    private KVIterateStore storeFile;
    private Exception ex;

    @Override
    public void setUp() throws IOException {
        if (!LogSetup.isActive()) {
            new LogSetup("./logs/test_ecs.log", Level.ALL);
        }
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

        // testing string with \r\n inside
        storeFile.put("wokeshuaile", "no problem\r\n");
        value = storeFile.get("wokeshuaile");
        assertTrue(value.equals("no problem\r\n"));

        storeFile.put("what", "you\rhuo");
        value = storeFile.get("what");
        assertTrue(value.equals("you\rhuo"));

        // test UTF-8
        storeFile.put("你好", "世界");
        value = storeFile.get("你好");
        assertTrue(value.equals("世界"));

        // test delete
        storeFile.put("wokeshuaile", "null");
        value = storeFile.get("wokeshuaile");
        assertNull(value);

        storeFile.put("hello", "null");
        value = storeFile.get("hello");
        assertTrue(value == null);

        // test modify
        storeFile.put("你好", "shijie");
        value = storeFile.get("你好");
        assertTrue(value.equals("shijie"));

        // delete an non existing key shall throw exception
        Exception ex = null;
        try {
            storeFile.put("wocao", "null");
        } catch (Exception e) {
            ex = e;
        }
        assertNotNull(ex);

        value = storeFile.get("wocao");
        assertTrue(value == null);
        assertFalse(storeFile.inStorage("wocao"));

        storeFile.put("shei", "bushiwo");
        value = storeFile.get("shei");
        assertTrue(value.equals("bushiwo"));
        assertTrue(storeFile.inStorage("shei"));

        // test with large string 100kb
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 1000 + 1; i++) {
            sb.append("a");
        }
        String testString = sb.toString();
        storeFile.put("large", testString);
        value = storeFile.get("large");
        assertTrue(value.equals(testString));

        // insert many entries
        for (int i = 0; i < 100; i++){
            storeFile.put("k" + Integer.toString(i), "v" + Integer.toString(i));
        }

        for (int i = 0; i < 100; i+=5){
            value = storeFile.get("k" + Integer.toString(i));
            assertNotNull(value);
            assertTrue(("v"+Integer.toString(i)).equals(value));
        }

        // change values
        for (int i = 0; i < 100; i+=2){
            storeFile.put("k" + Integer.toString(i), "cv" + Integer.toString(i));
        }

        for (int i = 0; i < 100; i+=2){
            value = storeFile.get("k" + Integer.toString(i));
            assertNotNull(value);
            assertTrue(("cv"+Integer.toString(i)).equals(value));
        }
        // delete entries
        for (int i = 0; i < 100; i+=5){
            storeFile.put("k" + Integer.toString(i), "null");
        }
        for (int i = 0; i < 100; i+=5){
            value = storeFile.get("k" + Integer.toString(i));
            assertNull(value);
        }

        // change the large test to a small one
        storeFile.put("large", "small");
        value = storeFile.get("large");
        assertTrue(value.equals("small"));


    }

    @Test
    public void testPreSend() {
        MessageDigest md = null;
        try {
            md = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }


        //generate a hash range
        md.reset();
        md.update(("Hello").getBytes());
        BigInteger val = new BigInteger(1, md.digest());
        String lower = val.toString(16);

        md.update(("World").getBytes());
        val = new BigInteger(1, md.digest());
        String upper = val.toString(16);

        String[] hashRange = new String[2];
        hashRange[0] = lower;
        hashRange[1] = upper;
        storeFile.preMoveData(hashRange);

        try {
            String value = storeFile.get("k99");
            assertTrue(value.equals("v99"));
            assertTrue(storeFile.inStorage("k99"));
        } catch (Exception e) {
            ex = e;
        }


    }

    @Test
    public void testAfterSend(){
        storeFile.afterMoveData();

        try {
            String value = storeFile.get("k94");
            assertTrue(value.equals("cv94"));
            assertTrue(storeFile.inStorage("k94"));
        } catch (Exception e) {
            ex = e;
        }

    }



}
