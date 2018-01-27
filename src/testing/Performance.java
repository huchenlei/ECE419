package testing;

import java.util.Random;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import app_kvServer.KVServer;
import logger.LogSetup;

public class Performance {
	
	private static Logger logger = Logger.getRootLogger();
	private long getLatency;
	private long putLatency;
	private int number_of_get;
	private int number_of_put;
	private KVServer server;
	
	public Performance (int port, int cacheSize, String strategy, String fileName) {
		this.getLatency=0;
		this.putLatency=0;
		this.number_of_get=0;
		this.number_of_put=0;
		server = new KVServer(port, cacheSize, strategy, fileName);
		server.clearStorage();
		new Thread(server).start();
	}
	
	public long getLatency() {
		return this.getLatency;
	}
	
	public long putLatency() {
		return this.putLatency;
	}
	
	public float totalLatency() {
		long totalLat = this.getLatency+this.putLatency;
		logger.info("Total Latency in mili seconds is:" + totalLat/1000000);
		return totalLat/1000000;
	}
	
	public float averageLatency() {
		float totalAvgLat = (this.getLatency+this.putLatency)/(number_of_get+number_of_put);
		logger.info("Total average latency in mili seconds is:" + totalAvgLat/1000000);
		return totalAvgLat/1000000;
	}
	
	public float averagePutLatency() {
		float averagePutLat = this.putLatency / this.number_of_put;
		logger.info("Average put latency in mili seconds is:" + averagePutLat/1000000 );
		return averagePutLat/1000000;
	}
	
	public float averageGetLatency() {
		float averageGetLat = this.getLatency / this.number_of_get;
		logger.info("Average get latency in mili seconds is:" + averageGetLat/1000000 );
		return averageGetLat/1000000;
	}
	
	public void recordLatency(int numRequest, int putPortion, int getPortion) {
		Random rand = new Random();
		try {
			this.number_of_get = (numRequest/(putPortion+getPortion))*getPortion;
			this.number_of_put = (numRequest/(putPortion+getPortion))*putPortion;
			int total = number_of_get+number_of_put;
			long start = System.nanoTime();
			for(int i=0; i<number_of_put; i++) {
				server.putKV(String.valueOf((rand.nextInt(total/10) + 1)), "testingLatency");
			}
			long end = System.nanoTime();
			this.putLatency = end-start;
			start = System.nanoTime();
			for(int i=0; i<number_of_get; i++) {
				server.getKV(String.valueOf((rand.nextInt(total/10) + 1)));
			}
			end = System.nanoTime();
			this.getLatency = end-start;	
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args)  {
		//define cache size here
		int cacheSize=200;
		//set it to be a multiple of 100
		//key will be randomly select from 1 - numRequest/10
		int numRequest=6000;
		int port = 20000;
		String cacheStrategy = "FIFO";
		int putPortion = 3;
		int getPortion = 7;
		try {
			 new LogSetup("logs/testing/performance.log", Level.INFO);
			 Performance performance = new Performance(port, cacheSize, cacheStrategy, "PerformanceDB");
			 performance.recordLatency(numRequest, putPortion, getPortion);
			 performance.totalLatency();
			 performance.averageLatency();
			 performance.averageGetLatency();
			 performance.averagePutLatency();	 
		}
		catch (Exception e) {
            e.printStackTrace();
        }
		
	}

}
