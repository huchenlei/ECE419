package ecs;

import app_kvECS.IECSClient;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * This class handles core functionality of external configuration service
 */
public class ECS implements IECSClient {
    private static final String SERVER_JAR = "KVServer.jar";
    // Assumes that the jar file is located at the same dir on the remote server
    private static final String JAR_PATH = new File(System.getProperty("user.dir"), SERVER_JAR).toString();

    private static Logger logger = Logger.getRootLogger();

    /**
     * Roster of all <b>idle<b/> storage service nodes
     */
    private Queue<IECSNode> nodePool = new ConcurrentLinkedQueue<>();

    /**
     * Roster of all service that are initialized(assigned cacheStrategy and cacheSize)
     */
    private Map<String, IECSNode> nodeTable = new HashMap<>();

    /**
     * HashRing object responsible to update the hash range of each ECSNode
     */
    private ECSHashRing hashRing = new ECSHashRing();

    public class ECSConfigFormatException extends RuntimeException {
        public ECSConfigFormatException(String msg) {
            super(msg);
        }
    }

    public ECS(String configFileName) throws IOException {
        BufferedReader configReader = new BufferedReader(new FileReader(new File(configFileName)));

        String currentLine;
        while ((currentLine = configReader.readLine()) != null) {
            String[] tokens = currentLine.split(" ");
            if (tokens.length != 3) {
                throw new ECSConfigFormatException("invalid number of arguments! should be 3 but got " +
                        tokens.length + ".");
            }
            ECSNode newNode = new ECSNode(tokens[0], tokens[1], Integer.parseInt(tokens[2]));
            nodePool.add(newNode);
            logger.info(newNode + " added to node pool");
        }
    }

    @Override
    public boolean start() throws Exception {
        logger.info("Starting all storage services");
        return false;
    }

    @Override
    public boolean stop() throws Exception {
        return false;
    }

    @Override
    public boolean shutdown() throws Exception {
        return false;
    }

    public IECSNode setupNode(String cacheStrategy, int cacheSize) {
        IECSNode n = nodePool.poll(); // node to activate
        hashRing.addNode((ECSNode) n);
        return n;
    }

    @Override
    public IECSNode addNode(String cacheStrategy, int cacheSize) {
        IECSNode n = setupNode(cacheStrategy, cacheSize);
        // Issue the ssh call to start the process remotely
        String javaCmd = String.join(" ",
                "java -jar",
                JAR_PATH,
                Integer.toString(n.getNodePort()),
                Integer.toString(cacheSize),
                cacheStrategy);
        String sshCmd = "ssh -o StrictHostKeyChecking=no -n " + n.getNodeHost() + " nohup " + javaCmd +
                " > ./logs/output.log 2> ./logs/err.log &";
        // Redirect output to files so that ssh channel will not wait for further output
        try {
            logger.info("Executing command: " + sshCmd);
            Process p = Runtime.getRuntime().exec(sshCmd);
            p.waitFor();
            assert !p.isAlive();
            assert p.exitValue() == 0;
        } catch (IOException e) {
            logger.error("Unable to launch server with ssh (" + n + ")", e);
            e.printStackTrace();
        } catch (InterruptedException e) {
            logger.error("Receive an interrupt", e);
            e.printStackTrace();
        }
        return n;
    }

    @Override
    public Collection<IECSNode> addNodes(int count, String cacheStrategy, int cacheSize) {
        List<IECSNode> nodeList = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            nodeList.add(addNode(cacheStrategy, cacheSize));
        }
        return nodeList;
    }

    @Override
    public Collection<IECSNode> setupNodes(int count, String cacheStrategy, int cacheSize) {
        List<IECSNode> nodeList = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            nodeList.add(setupNode(cacheStrategy, cacheSize));
        }
        return nodeList;
    }

    @Override
    public boolean awaitNodes(int count, int timeout) throws Exception {
        return false;
    }

    @Override
    public boolean removeNodes(Collection<String> nodeNames) {
        return false;
    }

    @Override
    public Map<String, IECSNode> getNodes() {
        return nodeTable;
    }

    @Override
    public IECSNode getNodeByKey(String key) {
        return hashRing.getNodeByKey(key);
    }

}
