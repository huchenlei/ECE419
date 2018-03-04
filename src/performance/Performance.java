package performance;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import app_kvServer.KVServer;
import client.KVStore;
import common.messages.KVMessage;
import ecs.ECS;
import ecs.IECSNode;
import logger.LogSetup;

public class Performance {
	
	private static Logger logger = Logger.getRootLogger();
	private long totalGetLatency;
	private long totalPutLatency;
	private int numReqests; 
	private Integer numer_of_servers;
	private Integer numer_of_clients;
	
	private ECS ecs = null;
    private String CACHE_STRATEGY;
    private Integer CACHE_SIZE;
    private List<KVServer> servers = new ArrayList<>();
    private List<KVStore> clients = new ArrayList<>();
    private List<ClientSession> sessions = new ArrayList<>();
    private List<KVMessage> msgs = DataParser.parseDataFrom("allen-p");
	
	public Performance (int cacheSize, String strategy, Integer numNodes, 
			Integer numClients, int numReq) throws Exception {
		this.totalGetLatency=0;
		this.totalPutLatency=0;
		this.numReqests = numReq;
		this.numer_of_servers = numNodes;
		this.numer_of_clients = numClients;
		
	    this.CACHE_SIZE = cacheSize;
	    this.CACHE_STRATEGY = strategy;
	    //initialize and start the required number of servers
		ecs = new ECS("./ecs.config");
		assertNotNull(ecs);
		this.addNodes(numNodes);
		boolean ret = ecs.start();
        assertTrue(ret);
        Thread.sleep(1000);
        //initialize the required number of clients
		int i, j;
		Random rand = new Random();
		for (i=0; i<numClients; i++) {
			//get a random server first to connect
			j = rand.nextInt(numNodes);
			KVStore store = new KVStore(servers.get(j).getHostname(), servers.get(j).getPort());
            store.connect();
            clients.add(store);
		}
	}
	
    public void addNodes(Integer count) throws Exception {
        Collection<IECSNode> nodes = ecs.setupNodes(count, this.CACHE_STRATEGY, this.CACHE_SIZE);
        assertNotNull(nodes);
        assertEquals(count, new Integer(nodes.size()));
        // Start the servers internally
        for (IECSNode node : nodes) {
            KVServer server = new KVServer(node.getNodePort(), node.getNodeName(),
                    ECS.ZK_HOST, Integer.parseInt(ECS.ZK_PORT));
            servers.add(server);
            server.clearStorage();
            new Thread(server).start();
        }
        boolean ret = ecs.awaitNodes(count, ECS.ZK_TIMEOUT);
        assertTrue(ret);
    }
    
    public void startTest() throws Exception {
    		int num = this.numReqests / this.clients.size(); 
    		for (KVStore client : clients) {
    			ClientSession session = new ClientSession(client, this.msgs, num);
    			this.sessions.add(session);
    		}
    		for (ClientSession session : sessions) {
    			session.start();
    		}
    		//Let all the sessions finish running
    		Thread.sleep(20000);
    		while (true) {
    			if (this.sessions.isEmpty()) {
    				break;
    			}
    			for (Iterator<ClientSession> iterator = sessions.iterator(); iterator.hasNext(); ) {
    				ClientSession session = iterator.next();
    				if (session.finishFlag()) {
    					this.totalGetLatency += session.getLantency();
    					this.totalPutLatency += session.putLantency();
    					iterator.remove();
    				}
    			}
    		}
    }
	
	public float totalLatency() {
		long totalLat = this.totalGetLatency+this.totalPutLatency;
		System.out.println("Total Latency in mili seconds is:" + totalLat/1000000);
		return totalLat/1000000;
	}
	
	public float averageLatency() {
		float totalAvgLat = (this.totalGetLatency+this.totalPutLatency)/(this.numReqests);
		System.out.println("Total average latency in mili seconds is:" + totalAvgLat/1000000);
		return totalAvgLat/1000000;
	}
	
	public float averagePutLatency() {
		float averagePutLat = this.totalPutLatency / (this.numReqests/2);
		System.out.println("Average put latency in mili seconds is:" + averagePutLat/1000000 );
		return averagePutLat/1000000;
	}
	
	public float averageGetLatency() {
		float averageGetLat = this.totalGetLatency / (this.numReqests/2);
		System.out.println("Average get latency in mili seconds is:" + averageGetLat/1000000 );
		return averageGetLat/1000000;
	}
	
	public void Shutdown() throws Exception {
        boolean ret = this.ecs.shutdown();
        assertTrue(ret);
    }
	
	public static void main(String[] args)  {
		//define cache size here
		int cacheSize=50;
		//ensure numRequest / numClients is a integer
		//ensure numRequest is an even number
		int numRequest=1000;
		String cacheStrategy = "FIFO";
		int numClients = 5;
		int numServers = 5;

		try {
			 new LogSetup("logs/testing/performance.log", Level.ERROR);
			 Performance performance = new Performance(cacheSize, cacheStrategy, 
					 numServers, numClients, numRequest);
			 performance.startTest();
			 performance.totalLatency();
			 performance.averageLatency();
			 performance.averageGetLatency();
			 performance.averagePutLatency();
			 performance.Shutdown();
		}
		catch (Exception e) {
            e.printStackTrace();
        }
		
	}

}


class ClientSession extends Thread {
	private List<KVMessage> messages;
	private KVStore client;
	private int numReq;
	private boolean finishRequests;
	private long getLatency;
	private long putLatency;
	private int size;
	
	public ClientSession(KVStore store, List<KVMessage> msgs, int num) {
		this.messages = msgs;
		this.client = store;
		this.numReq = num;
		this.getLatency = 0;
		this.putLatency = 0;
		this.size = this.messages.size();
		this.finishRequests = false;
	}
	
	public long getLantency() {
		return this.getLatency;
	}
	
	public long putLantency() {
		return this.putLatency;
	}
	
	public boolean finishFlag() {
		return this.finishRequests;
	}
	
	@Override
	public void run(){  
		Random rand = new Random();
		try {
			int index;
			long start = System.nanoTime();
			for(int i=0; i< numReq/2; i++) {
				index = rand.nextInt(this.size);
				client.put(messages.get(index).getKey(), messages.get(index).getValue());
			}
			long end = System.nanoTime();
			this.putLatency = end-start;
			start = System.nanoTime();
			for(int i=0; i< numReq/2; i++) {
				index = rand.nextInt(this.size);
				client.get(messages.get(index).getKey());
			}
			end = System.nanoTime();
			this.getLatency = end-start;	
			this.finishRequests = true;
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	} 
}
