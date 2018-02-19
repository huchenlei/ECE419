package ecs;

/**
 * Pojo class representing node in ecs
 */
public class ECSNode implements IECSNode {
    private String name;
    private String host;
    private Integer port;

    public ECSNode(String name, String host, Integer port) {
        this.name = name;
        this.host = host;
        this.port = port;
    }

    @Override
    public String getNodeName() {
        return name;
    }

    @Override
    public String getNodeHost() {
        return host;
    }

    @Override
    public int getNodePort() {
        return port;
    }

    @Override
    public String[] getNodeHashRange() {
        // TODO
        return new String[0];
    }
}
