package ecs;

import java.math.BigInteger;

/**
 * This class wrap the ECS Nodes and provide an LinkedList container
 * for adding and removing nodes
 */
public class ECSHashRing {
    private ECSNode root = null;

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
        }
        return currentNode;
    }

    /**
     * Add an node to hash ring
     * Complexity O(n)
     *
     * @param node ecsnode instance
     */
    public void addNode(ECSNode node) {
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
        ECSNode toRemove = getNodeByKey(hash);

        ECSNode next = getNodeByKey(hash + 1);

        if (toRemove.equals(next)
                || !(next.getPrev().equals(toRemove))) {
            throw new HashRingException("Invalid node hash value!");
        }

        next.setPrev(toRemove.getPrev());
        if (root.equals(toRemove)) {
            root = next;
        }
    }

    public class HashRingException extends RuntimeException {
        public HashRingException(String msg) {
            super(msg);
        }
    }
}
