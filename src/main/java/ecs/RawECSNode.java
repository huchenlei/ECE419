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

    /**
     * Number of clients connecting to the node
     */
    protected Integer connectionNumber = 0;
    /**
     * How many cache has been used
     */
    protected Integer cacheUtilization = 0;
    /**
     * Cache hit rate recently (recent 1000 access)
     */
    protected double cacheHitRate = 1.0;


    private String prevHash = null;

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
        if (n.getPrev() != null) {
            this.prevHash = n.getPrev().getNodeHash();
        }
    }
}
