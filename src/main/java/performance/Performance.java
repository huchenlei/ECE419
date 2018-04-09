package performance;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import app_kvServer.KVServer;
import client.KVStore;
import common.KVMessage;
import ecs.ECS;
import ecs.ECSNode;
import ecs.IECSNode;
import logger.LogSetup;

public class Performance {
	
	private static Logger logger = Logger.getRootLogger();
	private long totalGetLatency;
	private long totalPutLatency;
	private long add1NodesLatency;
	private long add5NodesLatency;
	private long add10NodesLatency;
	private long remove1NodesLatency;
	private long remove5NodesLatency;
	private long remove10NodesLatency;
	private int numReqests; 
	private Integer numer_of_servers;
	private Integer numer_of_clients;
	
	private ECS ecs = null;
    private String CACHE_STRATEGY;
    private Integer CACHE_SIZE;
    private List<KVStore> clients = new ArrayList<>();
    private Map<ECSNode, KVServer> serverTable = new HashMap<>();
    private List<ClientPutSession> putSessions = new ArrayList<>();
    private List<ClientGetSession> getSessions = new ArrayList<>();
    //SQL sessions defined below
    private long totalCreateLatency;
    private long totalInsertLatency;
    private long totalUpdateLatency;
    private long totalSelectLatency;
    private long totalDeleteLatency;
    private long totalDropLatency;
    
    private List<ClientCreateSession> createSessions = new ArrayList<>();
    private List<ClientDropSession> dropSessions = new ArrayList<>();
    private List<ClientSelectSession> selectSessions = new ArrayList<>();
    private List<ClientInsertSession> insertSessions = new ArrayList<>();
    private List<ClientUpdateSession> updateSessions = new ArrayList<>();
    private List<ClientDeleteSession> deleteSessions = new ArrayList<>();
    private List<KVMessage> msgs = DataParser.parseDataFrom("allen-p/all_documents");
	
	public Performance (int cacheSize, String strategy, Integer numNodes, 
			Integer numClients, int numReq) throws Exception {
		this.totalGetLatency=0;
		this.totalPutLatency=0;
		this.totalCreateLatency=0;
		this.totalDeleteLatency=0;
		this.totalDropLatency=0;
		this.totalInsertLatency=0;
		this.totalSelectLatency=0;
		this.totalUpdateLatency=0;
		this.numReqests = numReq;
		this.numer_of_servers = numNodes;
		this.numer_of_clients = numClients;
		
	    this.CACHE_SIZE = cacheSize;
	    this.CACHE_STRATEGY = strategy;
	    //initialize and start the required number of servers
		ecs = new ECS("ecs.config");
        ecs.clearRestoreList();
        ecs.locally = true;
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
			Object[] crunchifyKeys = serverTable.keySet().toArray();
			Object key = crunchifyKeys[new Random().nextInt(crunchifyKeys.length)];
			KVStore store = new KVStore(serverTable.get(key).getHostname(), serverTable.get(key).getPort());
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
            serverTable.put((ECSNode) node, server);
            server.clearStorage();
            new Thread(server).start();
        }
        boolean ret = ecs.awaitNodes(count, ECS.ZK_TIMEOUT);
        assertTrue(ret);
    }
    
    public void startSQLTest() throws Exception {
    		//clients size should be 1,5,25
    		//25 class tables, each class table has 50 students and their corresponding marks
    		int size = this.clients.size();
    		int j=0;
    		int[] classList = new int[25];
    		int[] subList = classList;
    		for (int i=0; i<25; i++) {
    			classList[i] = i+1;
    		}
    		
    		for (KVStore client : clients) {
    			if(size == 1) {
    				subList = classList;
    			}
    			if(size == 5) {
    				subList = Arrays.copyOfRange(classList, j*25/size, j*25/size+4);
    			}
    			if(size == 25) {
    				subList = Arrays.copyOfRange(classList, j*25/size, j*25/size);
    			}
			ClientCreateSession createSession = new ClientCreateSession(client,subList);
			this.createSessions.add(createSession);
			for (int num : subList) {
				ClientInsertSession insertSession = new ClientInsertSession(client,num);
				this.insertSessions.add(insertSession);
				ClientUpdateSession updateSession = new ClientUpdateSession(client,num);
				this.updateSessions.add(updateSession);
				ClientSelectSession selectSession = new ClientSelectSession(client,num);
				this.selectSessions.add(selectSession);
				ClientDeleteSession deleteSession = new ClientDeleteSession(client,num);
				this.deleteSessions.add(deleteSession);
			}
			ClientDropSession dropSession = new ClientDropSession(client,subList);
			this.dropSessions.add(dropSession);
    			j+=1;
    		}
    		//create latency
    		long start = System.nanoTime();
    		for (ClientCreateSession session : createSessions) {
    			session.start();
    		}
    		while (true) {
    			if (this.createSessions.isEmpty()) {
    				break;
    			}
    			//wait for the client session to be finished
        		Thread.sleep(100);
    			for (Iterator<ClientCreateSession> iterator = createSessions.iterator(); iterator.hasNext(); ) {
    				ClientCreateSession session = iterator.next();
    				if (session.finishFlag()) {
    					iterator.remove();
    				}
    			}
    		}
    		long end = System.nanoTime();
    		this.totalCreateLatency = end-start;
    		//insert latency
    		start = System.nanoTime();
    		for (ClientInsertSession session : insertSessions) {
    			session.start();
    		}
    		while (true) {
    			if (this.insertSessions.isEmpty()) {
    				break;
    			}
    			//wait for the client session to be finished
        		Thread.sleep(500);
    			for (Iterator<ClientInsertSession> iterator = insertSessions.iterator(); iterator.hasNext(); ) {
    				ClientInsertSession session = iterator.next();
    				if (session.finishFlag()) {
    					iterator.remove();
    				}
    			}
    		}
    		end = System.nanoTime();
    		this.totalInsertLatency = end-start;
    		
    		//add nodes and remove nodes here
    		this.addRemoveTest();
    		
    		//update latency
    		start = System.nanoTime();
    		for (ClientUpdateSession session : updateSessions) {
    			session.start();
    		}
    		while (true) {
    			if (this.updateSessions.isEmpty()) {
    				break;
    			}
    			//wait for the client session to be finished
        		Thread.sleep(500);
    			for (Iterator<ClientUpdateSession> iterator = updateSessions.iterator(); iterator.hasNext(); ) {
    				ClientUpdateSession session = iterator.next();
    				if (session.finishFlag()) {
    					iterator.remove();
    				}
    			}
    		}
    		end = System.nanoTime();
    		this.totalUpdateLatency = end-start;
    		//select latency
    		start = System.nanoTime();
    		for (ClientSelectSession session : selectSessions) {
    			session.start();
    		}
    		while (true) {
    			if (this.selectSessions.isEmpty()) {
    				break;
    			}
    			//wait for the client session to be finished
        		Thread.sleep(500);
    			for (Iterator<ClientSelectSession> iterator = selectSessions.iterator(); iterator.hasNext(); ) {
    				ClientSelectSession session = iterator.next();
    				if (session.finishFlag()) {
    					iterator.remove();
    				}
    			}
    		}
    		end = System.nanoTime();
    		this.totalSelectLatency = end-start;
    		//delete latency
    		start = System.nanoTime();
    		for (ClientDeleteSession session : deleteSessions) {
    			session.start();
    		}
    		while (true) {
    			if (this.deleteSessions.isEmpty()) {
    				break;
    			}
    			//wait for the client session to be finished
        		Thread.sleep(500);
    			for (Iterator<ClientDeleteSession> iterator = deleteSessions.iterator(); iterator.hasNext(); ) {
    				ClientDeleteSession session = iterator.next();
    				if (session.finishFlag()) {
    					iterator.remove();
    				}
    			}
    		}
    		end = System.nanoTime();
    		this.totalDeleteLatency = end-start;
    		
    		//drop latency
    		start = System.nanoTime();
    		for (ClientDropSession session : dropSessions) {
    			session.start();
    		}
    		while (true) {
    			if (this.dropSessions.isEmpty()) {
    				break;
    			}
    			//wait for the client session to be finished
        		Thread.sleep(100);
    			for (Iterator<ClientDropSession> iterator = dropSessions.iterator(); iterator.hasNext(); ) {
    				ClientDropSession session = iterator.next();
    				if (session.finishFlag()) {
    					iterator.remove();
    				}
    			}
    		}
    		end = System.nanoTime();
    		this.totalDropLatency = end-start;    		   		
    }
    
    public void startTest() throws Exception {
    		int num = this.numReqests / this.clients.size(); 
    		for (KVStore client : clients) {
    			ClientPutSession putSession = new ClientPutSession(client, this.msgs, num);
    			this.putSessions.add(putSession);
    			ClientGetSession getSession = new ClientGetSession(client, this.msgs, num);
    			this.getSessions.add(getSession);
    		}
    		//performance metrics for put requests
    		long start = System.nanoTime();
    		for (ClientPutSession sessionp : putSessions) {
    			sessionp.start();
    		}
    		while (true) {
    			if (this.putSessions.isEmpty()) {
    				break;
    			}
    			//wait for the client session to be finished
        		Thread.sleep(500);
    			for (Iterator<ClientPutSession> iterator = putSessions.iterator(); iterator.hasNext(); ) {
    				ClientPutSession session = iterator.next();
    				if (session.finishFlag()) {
    					iterator.remove();
    				}
    			}
    		}
    		long end = System.nanoTime();
    		this.totalPutLatency = end-start;
        
    		//performance metrics for get requests
    		start = System.nanoTime();
    		for (ClientGetSession sessiong : getSessions) {
    			sessiong.start();
    		}
    		while (true) {
    			if (this.getSessions.isEmpty()) {
    				break;
    			}
    			//wait for the client session to be finished
        		Thread.sleep(500);
    			for (Iterator<ClientGetSession> iterator = getSessions.iterator(); iterator.hasNext(); ) {
    				ClientGetSession session = iterator.next();
    				if (session.finishFlag()) {
    					iterator.remove();
    				}
    			}
    		}
    		end = System.nanoTime();
    		this.totalGetLatency = end-start;
        
    		this.addRemoveTest();
    }
    
    public void addRemoveTest() throws Exception {
    		long start = System.nanoTime();
		this.addNodes(1);
		ecs.start();
		long end = System.nanoTime();
		this.add1NodesLatency = end - start;
		Thread.sleep(200);
        
		start = System.nanoTime();
		this.addNodes(5);
		ecs.start();
		end = System.nanoTime();
		this.add5NodesLatency = end - start;
		Thread.sleep(200);
    
     
		start = System.nanoTime();
		this.addNodes(10);
		ecs.start();
		end = System.nanoTime();
		this.add10NodesLatency = end - start;
		Thread.sleep(200);
    
    
		start = System.nanoTime();
		this.RemoveNodes(1);
		end = System.nanoTime();
		this.remove1NodesLatency = end - start;
		Thread.sleep(200);
	
		start = System.nanoTime();
		this.RemoveNodes(5);
		end = System.nanoTime();
		this.remove5NodesLatency = end - start;
		Thread.sleep(200);
	
		start = System.nanoTime();
		this.RemoveNodes(10);
		end = System.nanoTime();
		this.remove10NodesLatency = end - start;
		Thread.sleep(200);
    }
    
    public void RemoveNodes(Integer number) throws Exception {
        ArrayList<ECSNode> nodes = new ArrayList<>(serverTable.keySet());
        // Remove the first two nodes
        List<ECSNode> toRemove = nodes.subList(0, number);
        boolean ret = ecs.removeNodes(
                toRemove.stream().map(ECSNode::getNodeName)
                        .collect(Collectors.toList()));
        assertTrue(ret);
        toRemove.forEach(serverTable::remove);
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
	
	public float get1AddNodesTime() {
		System.out.println("Add One Node Latency in mili seconds is:" + this.add1NodesLatency/1000000);
		return this.add1NodesLatency/1000000;
	}
	public float get5AddNodesTime() {
		System.out.println("Add Five Node Latency in mili seconds is:" + this.add5NodesLatency/1000000);
		return this.add5NodesLatency/1000000;
	}
	public float get10AddNodesTime() {
		System.out.println("Add Ten Node Latency in mili seconds is:" + this.add10NodesLatency/1000000);
		return this.add10NodesLatency/1000000;
	}
	
	public float get1RemoveNodesTime() {
		System.out.println("Remove One Node Latency in mili seconds is:" + this.remove1NodesLatency/1000000);
		return this.remove1NodesLatency/1000000;
	}
	public float get5RemoveNodesTime() {
		System.out.println("Remove Five Node Latency in mili seconds is:" + this.remove5NodesLatency/1000000);
		return this.remove5NodesLatency/1000000;
	}
	public float get10RemoveNodesTime() {
		System.out.println("Remove Ten Node Latency in mili seconds is:" + this.remove10NodesLatency/1000000);
		return this.remove10NodesLatency/1000000;
	}
	
	public float averageCreateLatency() {
		float averageCreateLat = this.totalCreateLatency / 25;
		System.out.println("Average create latency in mili seconds is:" + averageCreateLat/1000000 );
		return averageCreateLat/1000000;
	}
	
	public float averageDropLatency() {
		float averageDropLat = this.totalDropLatency / 25;
		System.out.println("Average drop latency in mili seconds is:" + averageDropLat/1000000 );
		return averageDropLat/1000000;
	}
	
	public float averageInsertLatency() {
		float averageInsertLat = this.totalInsertLatency / (25*50);
		System.out.println("Average insert latency in mili seconds is:" + averageInsertLat/1000000 );
		return averageInsertLat/1000000;
	}
	
	public float averageUpdateLatency() {
		float averageUpdateLat = this.totalUpdateLatency / (25*50);
		System.out.println("Average update latency in mili seconds is:" + averageUpdateLat/1000000 );
		return averageUpdateLat/1000000;
	}
	
	public float averageDeleteLatency() {
		float averageDeleteLat = this.totalDeleteLatency / (25*50);
		System.out.println("Average delete latency in mili seconds is:" + averageDeleteLat/1000000 );
		return averageDeleteLat/1000000;
	}
	
	public float averageSelectLatency() {
		float averageSelectLat = this.totalSelectLatency / (25*50);
		System.out.println("Average select latency in mili seconds is:" + averageSelectLat/1000000 );
		return averageSelectLat/1000000;
	}
	
	
	public void Shutdown() throws Exception {
        boolean ret = this.ecs.shutdown();
        Thread.sleep(2000);
        assertTrue(ret);
    }
	
	public static void main(String[] args)  {
		//define cache size here
		int cacheSize=30;
		//ensure numRequest / numClients is a integer
		//ensure numRequest is an even number
		int numRequest=10;
		String cacheStrategy = "FIFO";
		int numClients = 1;
		int numServers = 1;

		try {
			 new LogSetup("logs/testing/performance.log", Level.ERROR);
			 Performance performance = new Performance(cacheSize, cacheStrategy, 
					 numServers, numClients, numRequest);
			 //performance.startTest();
			 performance.startSQLTest();
			 //latency to add 1, 5, 10 nodes
			 performance.get1AddNodesTime();
			 performance.get5AddNodesTime();
			 performance.get10AddNodesTime();
			 performance.get1RemoveNodesTime();
			 performance.get5RemoveNodesTime();
			 performance.get10RemoveNodesTime();
			 performance.averageCreateLatency();
			 performance.averageSelectLatency();
			 performance.averageInsertLatency();
			 performance.averageUpdateLatency();
			 performance.averageDeleteLatency();
			 performance.averageDropLatency();
			 /*
			 performance.averageLatency();
			 performance.averageGetLatency();
			 performance.averagePutLatency();
			 */
			 performance.Shutdown();
		}
		catch (Exception e) {
            e.printStackTrace();
        }
		
	}

}

class ClientGetSession extends Thread {
	private List<KVMessage> messages;
	private KVStore client;
	private int numReq;
	private boolean finishRequests;
	private int size;
	
	public ClientGetSession(KVStore store, List<KVMessage> msgs, int num) {
		this.messages = msgs;
		this.client = store;
		this.numReq = num;
		this.size = this.messages.size();
		this.finishRequests = false;
	}
	
	public boolean finishFlag() {
		return this.finishRequests;
	}
	
	@Override
	public void run(){  
		Random rand = new Random();
		try {
			int index;
			for(int i=0; i< numReq/2; i++) {
				index = rand.nextInt(this.size);
				client.get(messages.get(index).getKey());
			}
			this.finishRequests = true;
	
		} catch (Exception e) {
			e.printStackTrace();
		}
	} 
}

class ClientPutSession extends Thread {
	private List<KVMessage> messages;
	private KVStore client;
	private int numReq;
	private boolean finishRequests;
	private int size;
	
	public ClientPutSession(KVStore store, List<KVMessage> msgs, int num) {
		this.messages = msgs;
		this.client = store;
		this.numReq = num;
		this.size = this.messages.size();
		this.finishRequests = false;
	}
	
	public boolean finishFlag() {
		return this.finishRequests;
	}
	
	@Override
	public void run(){  
		Random rand = new Random();
		try {
			int index;
			for(int i=0; i< numReq/2; i++) {
				index = rand.nextInt(this.size);
				client.put(messages.get(index).getKey(), messages.get(index).getValue());
			}
			this.finishRequests = true;
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	} 
}

class ClientCreateSession extends Thread {
	private KVStore client;
	private int[] number;
	private boolean finishRequests;
	
	public ClientCreateSession(KVStore store, int[] classNumber) {
		this.client = store;
		this.number = classNumber;
		this.finishRequests = false;
	}
	
	public boolean finishFlag() {
		return this.finishRequests;
	}
	
	@Override
	public void run(){  
		try {
			for(int i=0; i< number.length; i++) {
				client.sql("create class" + Integer.toString(number[i]) + " name:string,mark:number");
			}
			this.finishRequests = true;
	
		} catch (Exception e) {
			e.printStackTrace();
		}
	} 
}

class ClientDropSession extends Thread {
	private KVStore client;
	private int[] number;
	private boolean finishRequests;
	
	public ClientDropSession(KVStore store, int[] classNumber) {
		this.client = store;
		this.number = classNumber;
		this.finishRequests = false;
	}
	
	public boolean finishFlag() {
		return this.finishRequests;
	}
	
	@Override
	public void run(){  
		try {
			for(int i=0; i< number.length; i++) {
				client.sql("drop class" + Integer.toString(number[i]));
			}
			this.finishRequests = true;
	
		} catch (Exception e) {
			e.printStackTrace();
		}
	} 
}

class ClientInsertSession extends Thread {
	private KVStore client;
	private int number;
	private int mark;
	private boolean finishRequests;
	
	public ClientInsertSession(KVStore store, int classNumber) {
		this.client = store;
		this.number = classNumber;
		this.finishRequests = false;
	}
	
	public boolean finishFlag() {
		return this.finishRequests;
	}
	
	@Override
	public void run(){  
		try {
			Random rand = new Random();
			//insert 50 students to each class
			for(int i=0; i< 50; i++) {
				mark = rand.nextInt(100);
				String studentJson = "{'name':'student" + Integer.toString(i) + "','mark':" + mark + "}";
				client.sql("insert " + studentJson + " to class" + Integer.toString(number));
			}
			this.finishRequests = true;
	
		} catch (Exception e) {
			e.printStackTrace();
		}
	} 
}

class ClientUpdateSession extends Thread {
	private KVStore client;
	private int number;
	private int mark;
	private boolean finishRequests;
	
	public ClientUpdateSession(KVStore store, int classNumber) {
		this.client = store;
		this.number = classNumber;
		this.finishRequests = false;
	}
	
	public boolean finishFlag() {
		return this.finishRequests;
	}
	
	@Override
	public void run(){  
		try {
			Random rand = new Random();
			//update 50 students to each class
			for(int i=0; i< 50; i++) {
				mark = rand.nextInt(100);
				client.sql("update {'mark':" + mark + "} to class" + Integer.toString(number)+ 
						" where name = student" + Integer.toString(i));
			}
			this.finishRequests = true;
	
		} catch (Exception e) {
			e.printStackTrace();
		}
	} 
}

class ClientSelectSession extends Thread {
	private KVStore client;
	private int number;
	private boolean finishRequests;
	
	public ClientSelectSession(KVStore store, int classNumber) {
		this.client = store;
		this.number = classNumber;
		this.finishRequests = false;
	}
	
	public boolean finishFlag() {
		return this.finishRequests;
	}
	
	@Override
	public void run(){  
		try {
			for(int i=0; i< 50; i++) {
				client.sql("select mark from class" + Integer.toString(number) + 
						" where name = student"+ Integer.toString(i));
			}
			this.finishRequests = true;
	
		} catch (Exception e) {
			e.printStackTrace();
		}
	} 
}

class ClientDeleteSession extends Thread {
	private KVStore client;
	private int number;
	private boolean finishRequests;
	
	public ClientDeleteSession(KVStore store, int classNumber) {
		this.client = store;
		this.number = classNumber;
		this.finishRequests = false;
	}
	
	public boolean finishFlag() {
		return this.finishRequests;
	}
	
	@Override
	public void run(){  
		try {
			for(int i=0; i< 50; i++) {
				client.sql("delete from class" + Integer.toString(number) + 
						" where name = student"+ Integer.toString(i));
			}
			this.finishRequests = true;
	
		} catch (Exception e) {
			e.printStackTrace();
		}
	} 
}
