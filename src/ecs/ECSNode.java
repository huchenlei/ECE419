package ecs;

import org.apache.log4j.Logger;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

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

    public ECSNode(String name, String host, Integer port) {
        this.name = name;
        this.host = host;
        this.port = port;
    }

    public ECSNode(String name, String host, Integer port, ECSNode prev) {
        this(name, host, port);
        setPrev(prev);
    }

    public void setPrev(ECSNode node) {
        this.prev = node;
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
}
