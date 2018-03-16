package ecs;

import org.apache.log4j.Logger;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;

/**
 * Pojo class representing node in ecs
 */
public class ECSNode extends RawECSNode implements IECSNode {
    private static Logger logger = Logger.getRootLogger();
    private static MessageDigest md = null;

    static {
        try {
            md = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            logger.fatal("Unable to find hashing algorithm", e);
            e.printStackTrace();
        }
    }

    private String hash = null;
    private ECSNode prev;

    public enum ServerStatus {
        OFFLINE,
        INACTIVE, // ssh launched but not yet communicate with ECS yet
        STOP,
        ACTIVE,
    }

    private ServerStatus status;

    public ServerStatus getStatus() {
        return status;
    }

    public void setStatus(ServerStatus status) {
        this.status = status;
    }

    public ECSNode(String name, String host, Integer port) {
        super(name, host, port);
        this.status = ServerStatus.OFFLINE;
    }

    public ECSNode(String name, String host, Integer port, ECSNode prev) {
        this(name, host, port);
        setPrev(prev);
    }

    public ECSNode(RawECSNode rn) {
        this(rn.name, rn.host, rn.port);
    }

    public void setPrev(ECSNode node) {
        this.prev = node;
    }

    public ECSNode getPrev() {
        return this.prev;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ECSNode ecsNode = (ECSNode) o;
        return Objects.equals(host, ecsNode.host) &&
                Objects.equals(port, ecsNode.port);
    }

    @Override
    public int hashCode() {
        return Objects.hash(host, port);
    }

    @Override
    public String toString() {
        return "ECSNode{" +
                "hash='" + getNodeHash() + '\'' +
                ", status=" + status +
                ", name='" + name + '\'' +
                ", host='" + host + '\'' +
                ", port=" + port +
                '}';
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

    /**
     * Calculate the hash val of current node as hex string
     *
     * @return hex string (might not be 32 digits long because leading zeros are ignored)
     */
    public String getNodeHash() {
        assert host != null;
        assert port != null;
        if (this.hash == null) {
            this.hash = calcHash(host + ":" + port);
        }
        return this.hash;
    }

    public static synchronized String calcHash(String data) {
        md.reset();
        md.update(data.getBytes());
        BigInteger val = new BigInteger(1, md.digest());
        return val.toString(16);
    }

    @Override
    public String[] getNodeHashRange() {
        if (this.prev == null)
            return null;
        else
            return new String[]{
                    this.prev.getNodeHash(),
                    this.getNodeHash()
            };
    }



    public static boolean isKeyInRange(String key, String[] hexRange){
        String keyHash = calcHash(key);
        BigInteger lower = new BigInteger(hexRange[0], 16);
        BigInteger upper = new BigInteger(hexRange[1], 16);
        BigInteger k = new BigInteger(keyHash, 16);

        if (upper.compareTo(lower) <= 0) {
            // The node is responsible for ring end
            return k.compareTo(upper) <= 0 || k.compareTo(lower) > 0;
        } else {
            return k.compareTo(upper) <= 0 && k.compareTo(lower) > 0;
        }
    }
}
