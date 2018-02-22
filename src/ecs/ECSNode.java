package ecs;

import org.apache.log4j.Logger;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;

/**
 * Pojo class representing node in ecs
 */
public class ECSNode implements IECSNode {
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

    private String name;
    private String host;
    private Integer port;

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
        this.name = name;
        this.host = host;
        this.port = port ;
        this.status = ServerStatus.OFFLINE;
    }

    public ECSNode(String name, String host, Integer port, ECSNode prev) {
        this(name, host, port);
        setPrev(prev);
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
                "name='" + name + '\'' +
                ", host='" + host + '\'' +
                ", port=" + port +
                ", hash='" + hash + '\'' +
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
        if (this.hash == null) {
            md.reset();
            md.update((host + ":" + port).getBytes());
            BigInteger val = new BigInteger(1, md.digest());
            this.hash = val.toString(16);
        }
        return this.hash;
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
        md.reset();
        md.update((key).getBytes());
        BigInteger val = new BigInteger(1, md.digest());
        String keyHash = val.toString(16);
        BigInteger lower = new BigInteger(hexRange[0], 16);
        BigInteger upper = new BigInteger(hexRange[1], 16);
        BigInteger k = new BigInteger(keyHash, 16);

        if (upper.compareTo(lower) <= 0) {
            // The node is responsible for ring end
            if (k.compareTo(upper) <= 0 || k.compareTo(lower) > 0) {
                return true;
            }
        } else {
            if (k.compareTo(upper) <= 0 && k.compareTo(lower) > 0) {
                return true;
            }
        }

        return false;
    }
}
