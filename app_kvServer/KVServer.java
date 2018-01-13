package app_kvServer;

import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import server.KVServerConnection;

import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

public class KVServer implements IKVServer, Runnable {

    private static Logger logger = Logger.getRootLogger();

    private int port;
    private int cacheSize;
    private CacheStrategy strategy;

    private boolean running;
    private ServerSocket serverSocket;

    // TODO this is a dumb implementation of server storage
    // TODO might be used as an cache layer later
    private Map<String, String> dumbCache = new HashMap<>();

    /**
     * Start KV Server at given port
     *
     * @param port      given port for storage server to operate
     * @param cacheSize specifies how many key-value pairs the server is allowed
     *                  to keep in-memory
     * @param strategy  specifies the cache replacement strategy in case the cache
     *                  is full and there is a GET- or PUT-request on a key that is
     *                  currently not contained in the cache. Options are "FIFO", "LRU",
     *                  and "LFU".
     */
    public KVServer(int port, int cacheSize, String strategy) {
        this.port = port;
        this.cacheSize = cacheSize;
        this.strategy = CacheStrategy.valueOf(strategy);
    }

    @Override
    public int getPort() {
        return port;
    }

    @Override
    public String getHostname() {
        if (serverSocket == null) {
            return null;
        } else {
            return serverSocket.getInetAddress().getHostName();
        }
    }

    @Override
    public CacheStrategy getCacheStrategy() {
        return strategy;
    }

    @Override
    public int getCacheSize() {
        return cacheSize;
    }

    @Override
    public boolean inStorage(String key) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean inCache(String key) {
        return dumbCache.containsKey(key);
    }

    @Override
    public String getKV(String key) throws Exception {
        return dumbCache.get(key);
    }

    @Override
    public void putKV(String key, String value) throws Exception {
        dumbCache.put(key, value);
    }

    @Override
    public void clearCache() {
        dumbCache.clear();
    }

    @Override
    public void clearStorage() {
        // TODO Auto-generated method stub
    }

    /**
     * Save the cache to disk first and then close server socket
     */
    @Override
    public void kill() {
        running = false;
        try {
            serverSocket.close();
        } catch (IOException e) {
            logger.error("Unable to close socket on port: " + port, e);
        }
    }

    @Override
    public void close() {
        // TODO Other additional actions
        kill();
    }

    @Override
    public void run() {
        running = initializeServer();
        if (serverSocket != null) {
            while (isRunning()) {
                try {
                    Socket client = serverSocket.accept();
                    KVServerConnection conn = new KVServerConnection(this, client);
                    new Thread(conn).start();

                    logger.info("Connected to "
                            + client.getInetAddress().getHostName()
                            +  " on port " + client.getPort());
                } catch (IOException e) {
                    logger.error("Unable to establish connection with client.\n", e);
                }
            }
        }
    }

    public boolean isRunning() {
        return running;
    }

    private boolean initializeServer() {
        logger.info("Initialize server ...");
        try {
            serverSocket = new ServerSocket(port);
            logger.info("Server listening on port: "
                    + serverSocket.getLocalPort());
            return true;
        } catch (IOException e) {
            logger.error("Error! Cannot open server socket:");
            if (e instanceof BindException) {
                logger.error("Port " + port + " is already bound!");
            }
            return false;
        }
    }

    /**
     * Main entry point for the KVServer application.
     *
     * @param args contains the port number at args[0].
     */
    public static void main(String[] args) {
        try {
            new LogSetup("logs/server.log", Level.ALL);
            if (args.length < 1) {
                System.err.println("Error! Invalid number of arguments!");
                System.err.println("Usage: Server <port> (<cache size>) (<strategy>)!");
            } else {
                new Thread(new KVServer(
                        Integer.parseInt(args[0]),
                        Integer.parseInt(args[1]),
                        args[2]
                )).start();
            }
        } catch (NumberFormatException nfe) {
            System.err.println("Error! Invalid <port> or Invalid <cache size>! Not a number!");
            System.exit(1);
        } catch (IllegalArgumentException iae) {
            System.err.println("Error! Invalid <strategy>! Must be one of [None LRU LFU FIFO]!");
            System.exit(1);
        } catch (IOException ioe) {
            System.err.println("Error! Unable to initialize logger!");
            ioe.printStackTrace();
            System.exit(1);
        }
    }
}
