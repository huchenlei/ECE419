package testing;

import org.junit.Test;

import client.KVStore;
import junit.framework.TestCase;
import common.KVMessage;
import common.KVMessage.StatusType;


public class InteractionTest extends TestCase {

    private KVStore kvClient;

    public void setUp() {
        kvClient = new KVStore("localhost", 50099);
        try {
            kvClient.connect();
        } catch (Exception e) {
        }
    }

    public void tearDown() {
        kvClient.disconnect();
    }

    @Test
    public void testPut() {
        String key = "foo2";
        String value = "bar2";
        KVMessage response = null;
        Exception ex = null;

        try {
            response = kvClient.put(key, value);
        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex == null && response.getStatus() == StatusType.PUT_SUCCESS);
    }

    @Test
    public void testPutDisconnected() {
        kvClient.disconnect();
        String key = "foo";
        String value = "bar";
        Exception ex = null;

        try {
            kvClient.put(key, value);
        } catch (Exception e) {
            ex = e;
        }

        assertNotNull(ex);
    }

    @Test
    public void testUpdate() {
        String key = "updateTestValue";
        String initialValue = "initial";
        String updatedValue = "updated";

        KVMessage response = null;
        Exception ex = null;

        try {
            kvClient.put(key, initialValue);
            response = kvClient.put(key, updatedValue);

        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex == null && response.getStatus() == StatusType.PUT_UPDATE
                && response.getValue().equals(updatedValue));
    }

    @Test
    public void testDelete() {
        String key = "deleteTestValue";
        String value = "toDelete";

        KVMessage response = null;
        Exception ex = null;

        try {
            kvClient.put(key, value);
            response = kvClient.put(key, "null");

        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex == null && response.getStatus() == StatusType.DELETE_SUCCESS);
    }

    @Test
    public void testGet() {
        String key = "foo";
        String value = "bar";
        KVMessage response = null;
        Exception ex = null;

        try {
            kvClient.put(key, value);
            response = kvClient.get(key);
        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex == null && response.getValue().equals("bar"));
    }

    @Test
    public void testGetUnsetValue() {
        String key = "an unset value";
        KVMessage response = null;
        Exception ex = null;

        try {
            response = kvClient.get(key);
        } catch (Exception e) {
            ex = e;
        }

        assertTrue(ex == null && response.getStatus() == StatusType.GET_ERROR);
    }

    // Following are extra tests added
    private static final Integer MAX_VALUE_SIZE = 120 * 1024;
    @Test
    public void testBadValues() throws Exception {
        // Max val is 120 kb
        // build a string of 120kb + 1byte long
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < MAX_VALUE_SIZE + 1; i++) {
            sb.append("a");
        }

        String[] badVals = {sb.toString()};
        String key = "key";
        KVMessage res = null;
        for (String badVal :
                badVals) {
            res = kvClient.put(key, badVal);
            assertEquals(StatusType.PUT_ERROR, res.getStatus());
        }
    }

    public void testBadKeys() throws Exception {
        String[] badKeys = {
                // These keys are not allowed
                "",
                // key with 21 byte length
                "123456789012345678901",
        };
        String val = "val";
        KVMessage res = null;

        for (String badKey :
                badKeys) {
            res = kvClient.put(badKey, val);
            assertEquals(res.getStatus(), StatusType.PUT_ERROR);
        }

        String nonExKey = "not_exist_key";
        kvClient.put(nonExKey, "null"); // delete the key if already exist
        // Delete a non existing key shall report DELETE ERROR
        res = kvClient.put(nonExKey, "null");
        assertEquals(res.getStatus(), StatusType.DELETE_ERROR);

    }
}
