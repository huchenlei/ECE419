package testing;

import org.junit.Test;
import junit.framework.TestCase;
import server.cache.KVLFUCache;


public class LFUCacheTest extends TestCase{

	private KVLFUCache lfuCache;
	private static final Integer CACHE_SIZE = 5;
	
	
    @Override
    protected void setUp() throws Exception {
        super.setUp();
        lfuCache = new KVLFUCache(CACHE_SIZE);
    }
	
	@Test
	public void testLFU() {
		
		try {
			
			lfuCache.put("a", "hello1");
			lfuCache.put("b", "hello2");
			assertTrue(lfuCache.get("b").equals("hello2"));
			lfuCache.put("c", "hello3");
			lfuCache.put("d", "hello4");
			lfuCache.put("d", "hello44");
			assertTrue(lfuCache.get("d").equals("hello44"));
			lfuCache.put("e", "hello5");
			lfuCache.put("a", "hello11");
			lfuCache.put("f", "hello6");
			lfuCache.put("f", "hello66");
			assertTrue(lfuCache.get("f").equals("hello66"));
			lfuCache.put("g", "hello7");

			
			assertFalse(lfuCache.containsKey("c"));
			assertFalse(lfuCache.containsKey("e"));
			assertTrue(lfuCache.containsKey("a"));
			assertEquals(lfuCache.get("a"), "hello11");
			assertTrue(lfuCache.containsKey("b"));
			assertEquals(lfuCache.get("b"), "hello2");
			assertTrue(lfuCache.containsKey("d"));
			assertEquals(lfuCache.get("d"), "hello44");
			assertTrue(lfuCache.containsKey("f"));
			assertEquals(lfuCache.get("f"), "hello66");
			assertTrue(lfuCache.containsKey("g"));
			assertEquals(lfuCache.get("g"), "hello7");			
				
		} catch (Exception e) {
			 System.out.println(e.toString());
			 assertTrue(e == null);
		}
		
	}
		
}
