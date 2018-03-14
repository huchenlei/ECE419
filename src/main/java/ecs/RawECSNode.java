package ecs;

/**
 * Pojo class representing node in ecs
 */
public class RawECSNode {
    protected String name;
    protected String host;
    protected Integer port;
    protected ECSNode.ServerStatus status;
    protected String hash = null;

    public RawECSNode() {}

    public RawECSNode(String name, String host, Integer port) {
        this.name = name;
        this.host = host;
        this.port = port;
    }

    public RawECSNode(ECSNode n) {
        this(n.name, n.host, n.port);
        this.status = n.status;
        this.hash = n.getNodeHash();
    }
}
