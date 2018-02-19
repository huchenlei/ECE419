package ecs;

import app_kvECS.IECSClient;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;

/**
 * This class handles core functionality of external configuration service
 */
public class ECS implements IECSClient {


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
        }
    }

    @Override
    public boolean start() throws Exception {
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

    @Override
    public IECSNode addNode(String cacheStrategy, int cacheSize) {
        return null;
    }

    @Override
    public Collection<IECSNode> addNodes(int count, String cacheStrategy, int cacheSize) {
        return null;
    }

    @Override
    public Collection<IECSNode> setupNodes(int count, String cacheStrategy, int cacheSize) {
        return null;
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
        return null;
    }

    @Override
    public IECSNode getNodeByKey(String Key) {
        return null;
    }
}
