package testing;

import java.net.UnknownHostException;

import app_kvServer.KVServer;
import client.KVStore;

import junit.framework.TestCase;


public class ConnectionTest extends TestCase {

	
	public void testConnectionSuccess() {
		
		Exception ex = null;
		
		KVStore kvClient = new KVStore("localhost", 50000);
		try {
			kvClient.connect();
		} catch (Exception e) {
			ex = e;
		}	
		
		assertNull(ex);
	}
	
	
	public void testUnknownHost() {
		Exception ex = null;
		KVStore kvClient = new KVStore("unknown", 50000);
		
		try {
			kvClient.connect();
		} catch (Exception e) {
			ex = e; 
		}
		
		assertTrue(ex instanceof UnknownHostException);
	}
	
	
	public void testIllegalPort() {
		Exception ex = null;
		KVStore kvClient = new KVStore("localhost", 123456789);
		
		try {
			kvClient.connect();
		} catch (Exception e) {
			ex = e; 
		}
		
		assertTrue(ex instanceof IllegalArgumentException);
	}


	public void testInitialKVServer() {
		KVServer server1 = new KVServer("Server1", "localhost", 2181);
		KVServer server2 = new KVServer("Server2", "localhost", 2181);
		int port = server1.receiveData();

		server2.sendData(null,"localhost", port);
		try {
			Thread.sleep(10000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	
}

