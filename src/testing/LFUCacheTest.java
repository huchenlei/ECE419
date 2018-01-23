package testing;

import org.junit.Test;
import app_kvServer.KVServer;
import junit.framework.TestCase;
import logger.LogSetup;
import java.io.IOException;
import org.apache.log4j.Level;
import client.KVStore;
import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;


public class LFUCacheTest extends TestCase{
	
	private KVStore kvClient;
	private KVServer server;
	
	
	public void setUp() {
		try {
			new LogSetup("logs/testing/lfucache.log", Level.ERROR);
	        server = new KVServer(50001, 5, "LFU", "TestIterateDB");
	        server.clearStorage();
	        new Thread(server).start();
	        this.setClient();
		} catch (IOException e) {
			e.printStackTrace();
		}	
	}
	
	public void setClient() {
		kvClient = new KVStore("localhost", 50001);
		try {
			kvClient.connect();
		} catch (Exception e) {
		}
	}
	
	@Test
	public void testLFU() {
		KVMessage response = null;
		try {
			
			response = kvClient.put("a", "hello1");
			assertTrue(response.getStatus() == StatusType.PUT_SUCCESS);
			
			response = kvClient.put("b", "hello2");
			assertTrue(response.getStatus() == StatusType.PUT_SUCCESS);
			response = kvClient.get("b");
			assertTrue(response.getValue().equals("hello2"));
			
			response = kvClient.put("c", "hello3");
			assertTrue(response.getStatus() == StatusType.PUT_SUCCESS);
			
			response = kvClient.put("d", "hello4");
			assertTrue(response.getStatus() == StatusType.PUT_SUCCESS);
			response = kvClient.put("d", "hello44");
			assertTrue(response.getStatus() == StatusType.PUT_UPDATE);
			response = kvClient.get("d");
			assertTrue(response.getValue().equals("hello44"));
			
			response = kvClient.put("e", "hello5");
			assertTrue(response.getStatus() == StatusType.PUT_SUCCESS);
			
			response = kvClient.put("a", "hello11");
			assertTrue(response.getStatus() == StatusType.PUT_UPDATE);
			
			response = kvClient.put("f", "hello6");
			assertTrue(response.getStatus() == StatusType.PUT_SUCCESS);
			response = kvClient.put("f", "hello66");
			assertTrue(response.getStatus() == StatusType.PUT_UPDATE);
			response = kvClient.get("f");
			assertTrue(response.getValue().equals("hello66"));
			
			response = kvClient.put("g", "hello7");
			assertTrue(response.getStatus() == StatusType.PUT_SUCCESS);
			
			assertFalse(this.server.inCache("c"));
			assertFalse(this.server.inCache("e"));
			assertTrue(this.server.inCache("a"));
			assert this.server.getKV("a").equals("hello11");
			assertTrue(this.server.inCache("b"));
			assert this.server.getKV("b").equals("hello2");
			assertTrue(this.server.inCache("d"));
			assert this.server.getKV("d").equals("hello44");
			assertTrue(this.server.inCache("f"));
			assert this.server.getKV("f").equals("hello66");
			assertTrue(this.server.inCache("g"));
			assert this.server.getKV("g").equals("hello7");
				
		} catch (Exception e) {
			 System.out.println(e.toString());
			 assertTrue(e == null);
		}
		
	}
	
	public void tearDown() {
		kvClient.disconnect();
		server.close();
	}
		
}
