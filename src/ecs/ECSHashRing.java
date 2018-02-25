package ecs;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.log4j.Logger;

import java.math.BigInteger;
import java.util.Collection;
import java.util.List;

/**
 * This class wrap the ECS Nodes and provide an LinkedList container
 * for adding and removing nodes
 */
public class ECSHashRing {
    private Logger logger = Logger.getRootLogger();
    private ECSNode root = null;
    private Integer size = 0;
    public static final String LOOP_ERROR_STR =
            "Mal-formed structure detected; potentially causing infinite loop";

    public Integer getSize() {
        return size;
    }

    public ECSHashRing() {
    }

    /**
     * Initialize hash ring with given nodes
     *
     * @param nodes ECSNodes
     */
    public ECSHashRing(Collection<RawECSNode> nodes) {
        for (RawECSNode node : nodes) {
            addNode(new ECSNode(node));
        }
    }

    @SuppressWarnings("unchecked")
    public ECSHashRing(String jsonData) {
        this((List<RawECSNode>) new Gson().fromJson(jsonData,
                new TypeToken<List<RawECSNode>>() {
                }.getType()));
    }

    /**
     * Get the node responsible for given key
     * Complexity O(n)
     *
     * @param key md5 hash string
     */
    public ECSNode getNodeByKey(String key) {
        BigInteger k = new BigInteger(key, 16);
        ECSNode currentNode = root;
        if (root == null) return null;
        Integer loopCounter = 0;

        while (true) {
            String[] hashRange = currentNode.getNodeHashRange();
            assert hashRange != null;

            BigInteger lower = new BigInteger(hashRange[0], 16);
            BigInteger upper = new BigInteger(hashRange[1], 16);

            if (upper.compareTo(lower) <= 0) {
                // The node is responsible for ring end
                if (k.compareTo(upper) <= 0 || k.compareTo(lower) > 0) {
                    break;
                }
            } else {
                if (k.compareTo(upper) <= 0 && k.compareTo(lower) > 0) {
                    break;
                }
            }
            currentNode = currentNode.getPrev();

            if (loopCounter > 2 * size)
                throw new HashRingException(LOOP_ERROR_STR);
        }
        return currentNode;
    }

    private static final BigInteger BIG_ONE = new BigInteger("1", 16);

    public ECSNode getNextNode(ECSNode n) {
        return getNextNode(n.getNodeHash());
    }

    public ECSNode getNextNode(String h) {
        BigInteger hash = new BigInteger(h, 16);
        hash = hash.add(BIG_ONE);
        return getNodeByKey(hash.toString(16));
    }

    /**
     * Add an node to hash ring
     * Complexity O(n)
     *
     * @param node ecsnode instance
     */
    public void addNode(ECSNode node) {
        logger.info("Adding node " + node);
        if (root == null) {
            root = node;
            root.setPrev(node);
        } else {
            ECSNode loc = getNodeByKey(node.getNodeHash());
            ECSNode prev = loc.getPrev();

            assert prev != null;

            if (node.getNodeHash().equals(loc.getNodeHash())) {
                throw new HashRingException(
                        "Hash collision detected!\nloc: " + loc + "\nnode: " + node);
            }

            node.setPrev(prev);
            loc.setPrev(node);
        }
        this.size++;
    }

    public void removeNode(ECSNode node) {
        removeNode(node.getNodeHash());
    }

    /**
     * Remove an node from hash ring based on the node's hash value
     * Complexity O(n)
     *
     * @param hash md5 hash string
     */
    public void removeNode(String hash) {
        logger.info("Removing node with hash " + hash);
        ECSNode toRemove = getNodeByKey(hash);
        if (toRemove == null) {
            throw new HashRingException(
                    "HashRing empty! while attempting to move item from it");
        }
        ECSNode next = getNextNode(hash);
        assert next != null;

        if (toRemove.equals(next) && root.getNodeHash().equals(hash)) {
            // remove the last element in hash ring
            root = null;
            size--;
            assert size == 0;
            return;
        } else if ((toRemove.equals(next) && !root.getNodeHash().equals(hash))
                || !(next.getPrev().equals(toRemove))) {
            throw new HashRingException("Invalid node hash value! (" + hash + ")\nnext: "
                    + next + "\nthis: " + toRemove + "\nnext->prev: " + next.getPrev());
        }

        next.setPrev(toRemove.getPrev());
        if (root.equals(toRemove)) {
            root = next;
        }
        this.size--;
    }

    /**
     * Remove all nodes in HashRing
     */
    public void removeAll() {
        if (root == null) return;

        ECSNode currentNode = root;
        Integer loopCounter = 0;
        while (currentNode != null) {
            ECSNode prev = currentNode.getPrev();
            currentNode.setPrev(null);
            currentNode = prev;
            loopCounter++;

            if (loopCounter > 2 * size) {
                throw new HashRingException(LOOP_ERROR_STR);
            }
        }
        size = 0;
        root = null;
    }

    @Override
    public String toString() {
        if (root == null)
            return "ECSHashRing{}";

        StringBuilder sb = new StringBuilder();
        sb.append("ECSHashRing{\n");

        ECSNode currentNode = root;
        Integer loopCounter = 0;
        while (true) {
            sb.append(currentNode);
            sb.append("\n");
            currentNode = currentNode.getPrev();
            if (currentNode.equals(root))
                break;

            loopCounter++;
            if (loopCounter > 2 * size)
                throw new HashRingException(LOOP_ERROR_STR);
        }
        sb.append("}");
        return sb.toString();
    }

    public boolean empty() {
        return this.root == null;
    }

    public static class HashRingException extends RuntimeException {
        public HashRingException(String msg) {
            super(msg);
        }
    }
}
