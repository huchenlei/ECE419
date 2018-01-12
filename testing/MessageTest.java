package testing;

import common.messages.KVMessage;
import common.messages.KVMessageException;
import common.messages.SimpleKVMessage;
import junit.framework.TestCase;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.lang.reflect.Field;

/**
 * This testcase tests basic features of Message sub-classes
 * Created by Charlie on 2018-01-12.
 */
public class MessageTest extends TestCase {
    private Exception ex;
    private static String simpleDelim = ",";

// TODO somehow the @BeforeClass annotation does not work
// TODO Temporarily hardcode the delim to let other parts of test function
//    @BeforeClass
//    public static void init() throws Exception {
//        Field delimField = SimpleKVMessage.class.getDeclaredField("DELIM");
//        delimField.setAccessible(true);
//        simpleDelim = (String) delimField.get(SimpleKVMessage.class); // Pass null here since DELIM is a static field
//        assert(simpleDelim != null);
//    }

    @Before
    public void preTest() {
        ex = null;
    }

    @Test
    public void testSimpleKVEncodeSuccess() {
        SimpleKVMessage m = new SimpleKVMessage("test_key" + simpleDelim, "test_value" + simpleDelim, "GET");
        try {
            m.encode();
        } catch (KVMessageException e) {
            ex = e;
        }
        assertNull(ex);
    }

    @Test
    public void testSimpleKVEncodeEmpty() {
        SimpleKVMessage m = new SimpleKVMessage();
        try {
            m.encode();
        } catch (KVMessageException e) {
            ex = e;
        }
        assertNotNull(ex);
    }

    @Test
    public void testSimpleKVDecodeSuccess() {
        SimpleKVMessage m = new SimpleKVMessage();
        try {
            m.decode("test_--" + simpleDelim + "key,test_--" + simpleDelim + "value,GET");
        } catch (KVMessageException e) {
            ex = e;
        }
        assertNull(ex);
    }

    @Test
    public void testSimpleKVDecodeMal() {
        try {
            new SimpleKVMessage("test, test, GET, test");
        } catch (KVMessageException e) {
            ex = e;
        }

        assertNotNull(ex);
        ex = null;

        try {
            new SimpleKVMessage("test");
        } catch (KVMessageException e) {
            ex = e;
        }

        assertNotNull(ex);
    }

    /**
     * Calling encode shall not mutate the inner fields of message
     *
     * Decoding after encoding shall not change the message content
     */
    @Test
    public void testSimpleKVImmutable() {
        String key = "test_key--" + simpleDelim;
        String val = "test_value--" + simpleDelim;
        KVMessage.StatusType status = KVMessage.StatusType.DELETE_ERROR;
        SimpleKVMessage m1 = new SimpleKVMessage(key, val, status);

        SimpleKVMessage m2 = new SimpleKVMessage(m1.encode());

        assertEquals(key, m1.getKey());
        assertEquals(val, m1.getValue());
        assertEquals(status, m1.getStatus());

        assertEquals(key, m2.getKey());
        assertEquals(val, m2.getValue());
        assertEquals(status, m2.getStatus());
    }
}
