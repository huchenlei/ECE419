package ecs;

import org.apache.log4j.Logger;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

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

    private ECSNode prev;

    public String cacheStrategy;
    public Integer cacheSize;


    public enum ServerStatus {
        OFFLINE,
        INACTIVE, // ssh launched but not yet communicate with ECS yet
        STOP,
        ACTIVE,
    }

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

    public ECSNode(ECSNode n) {
        this(n.name, n.host, n.port, n.prev);
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

    /**
     * Returns whether the hash is in hashRange of current node
     *
     * @param hash hash string
     * @return boolean value
     */
    public boolean isHashInRange(String hash) {
        String[] hexRange = this.getNodeHashRange();
        if (hexRange == null) return false;
        return new HashRange(hexRange).inRange(hash);
    }

    public static boolean isKeyInRange(String key, String[] hexRange) {
        String keyHash = calcHash(key);
        return new HashRange(hexRange).inRange(keyHash);
    }

    public static class HashRange {
        private BigInteger lower;
        private BigInteger upper;

        public HashRange(String[] bounds) {
            this(bounds[0], bounds[1]);
        }

        public HashRange(String lower, String upper) {
            this(new BigInteger(lower, 16),
                    new BigInteger(upper, 16));
        }

        public HashRange(BigInteger lower, BigInteger upper) {
            this.lower = lower;
            this.upper = upper;
        }

        public HashRange(HashRange other) {
            this.lower = other.lower;
            this.upper = other.upper;
        }

        public String[] getStringRange() {
            return new String[]{
                    lower.toString(16),
                    upper.toString(16)
            };
        }

        public boolean inRange(String hash) {
            BigInteger k = new BigInteger(hash, 16);
            return inRange(k);
        }

        public boolean inRange(BigInteger k) {
            if (upper.compareTo(lower) <= 0) {
                // The node is responsible for ring end
                return k.compareTo(upper) <= 0 || k.compareTo(lower) > 0;
            } else {
                return k.compareTo(upper) <= 0 && k.compareTo(lower) > 0;
            }
        }

        public HashRange intersection(HashRange other) {
            boolean lowerInRange = inRange(other.lower);
            boolean upperInRange = inRange(other.upper);
            if (lowerInRange && upperInRange) {
                return new HashRange(other);
            } else if ((!lowerInRange) && (!upperInRange)) {
                return new HashRange(this);
            } else if (lowerInRange) {
                return new HashRange(other.lower, this.upper);
            } else {
                return new HashRange(this.lower, other.upper);
            }
        }

        public HashRange union(HashRange other) {
            boolean lowerInRange = inRange(other.lower);
            boolean upperInRange = inRange(other.upper);
            if (lowerInRange && upperInRange) {
                return new HashRange(this);
            } else if ((!lowerInRange) && (!upperInRange)) {
                return new HashRange(other);
            } else if (lowerInRange) {
                return new HashRange(this.lower, other.upper);
            } else {
                return new HashRange(other.lower, this.upper);
            }
        }

        public List<HashRange> remove(HashRange other) {
            boolean lowerInRange = inRange(other.lower);
            boolean upperInRange = inRange(other.upper);
            ArrayList<HashRange> result = new ArrayList<>();
            if (lowerInRange && upperInRange) {
                result.add(new HashRange(other.upper, this.upper));
                result.add(new HashRange(this.lower, other.lower));
            } else if ((lowerInRange) || (upperInRange)) {
                if (lowerInRange) {
                    result.add(new HashRange(this.lower, other.lower));
                } else {
                    result.add(new HashRange(other.upper, this.upper));
                }
            } else {
                result.add(new HashRange(this));
            }
            return result;
        }


        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            HashRange hashRange = (HashRange) o;
            return Objects.equals(lower, hashRange.lower) &&
                    Objects.equals(upper, hashRange.upper);
        }

        @Override
        public int hashCode() {
            return Objects.hash(lower, upper);
        }
    }
}
