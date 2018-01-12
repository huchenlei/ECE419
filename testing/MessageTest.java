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
    private String simpleDelim;

    @BeforeClass
    public void init() throws NoSuchFieldException, IllegalAccessException {
        Field delimField = SimpleKVMessage.class.getDeclaredField("DELIM");
        delimField.setAccessible(true);
        simpleDelim = (String) delimField.get(null); // Pass null here since DELIM is a static field
    }

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
            m.decode("test_\\" + simpleDelim + "key,test_\\" + simpleDelim + "value,GET");
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
     */
    @Test
    public void testSimpleKVImmutable() {
        String key = "test_key";
        String val = "test_value";
        KVMessage.StatusType status = KVMessage.StatusType.DELETE_ERROR;
        SimpleKVMessage m = new SimpleKVMessage(key, val, status);

        m.encode();

        assertEquals(key, m.getKey());
        assertEquals(val, m.getValue());
        assertEquals(status, m.getStatus());
    }
}
