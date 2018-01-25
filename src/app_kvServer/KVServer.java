package app_kvServer;

import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import server.KVIterateStore;
import server.KVPersistentStore;
import server.KVServerConnection;
import server.cache.KVCache;
import server.cache.KVFIFOCache;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
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

    /**
     * cache would be null if strategy is set to None
     */
    private KVCache cache;
    private KVPersistentStore store;

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
        this(port, cacheSize, strategy, "iterateDataBase"); // Default db name
    }

    public KVServer(int port, int cacheSize, String strategy, String fileName) {
        this.port = port;
        this.cacheSize = cacheSize;
        this.strategy = CacheStrategy.valueOf(strategy);
        if (this.strategy == CacheStrategy.None) {
            this.cache = null;
        } else {
            // Use reflection to dynamically initialize the cache based on strategy name
            try {
                Constructor<?> cons = Class.forName("server.cache.KV" + strategy + "Cache").getConstructor(Integer.class);
                this.cache = (KVCache) cons.newInstance(cacheSize);
            } catch (ClassNotFoundException |
                    NoSuchMethodException |
                    IllegalAccessException |
                    InstantiationException |
                    InvocationTargetException e) {
                logger.fatal("Component of KVServer is not found, please check the integrity of jar package");
                e.printStackTrace();
            }
        }
        this.store = new KVIterateStore(fileName);
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
        try {
            return store.inStorage(key);
        } catch (Exception e) {
            // when there is problem reading the file from disk
            // consider it as data not on disk
            logger.error("Unable to access data file on disk!");
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public boolean inCache(String key) {
        if (cache != null) {
            return cache.containsKey(key);
        } else {
            return false;
        }
    }

    @Override
    public String getKV(String key) throws Exception {
        if (cache != null) {
            if (this.inCache(key)) {
                return cache.get(key);
            } else {
                // Not in cache, read from disk and update cache
                String result = store.get(key);
                if (result != null) {
                    cache.put(key, result);
                }
                return result;
            }
        } else {
            return store.get(key);
        }
    }

    @Override
    public void putKV(String key, String value) throws Exception {
        // Update both cache and storage
        store.put(key, value);
        if (cache != null)
            cache.put(key, value);
    }

    @Override
    public void clearCache() {
        cache.clear();
    }

    @Override
    public void clearStorage() {
        store.clearStorage();
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
        // TODO Store everything in cache but not in storage
        // TODO Currently there shall not be any concern on this issue
        // TODO since we do store to disk prior to store to cache
    }

    @Override
    public void close() {
        kill();
        clearCache();
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
                            + " on port " + client.getPort());
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
            this.port = serverSocket.getLocalPort();
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
