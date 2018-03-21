package ecs;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class ECSDataDistributionManager {
    private ECSHashRing hashRing;

    public ECSDataDistributionManager(ECSHashRing hashRing) {
        this.hashRing = hashRing;
    }

    public List<ECSDataTransferIssuer> addNode(ECSNode node) {
        List<ECSDataTransferIssuer> result = new ArrayList<>();
        ECSNode senderCoordinator = this.hashRing.getNodeByKey(node.getNodeHash());

        hashRing.addNode(node);

        // Hash Ring empty
        if (senderCoordinator == null) return result;

        // handle coordinator data
        String[] responsibleRange = this.hashRing.getResponsibleRange(senderCoordinator);
        String coordinatorNodeHash = senderCoordinator.getNodeHash();
        assert responsibleRange.length == 2;
        assert responsibleRange[1].equals(coordinatorNodeHash);

        if (hashRing.getSize() <= ECSHashRing.REPLICATION_NUM + 1) {
            result.add(new ECSDataTransferIssuer(senderCoordinator, node, responsibleRange));
            return result;
        }

        String[] transferRange = new String[]{node.getNodeHash(), responsibleRange[0]};
        result.add(new ECSDataTransferIssuer(senderCoordinator, node,
                transferRange));

        // handle replication data (delete)
        Collection<ECSNode> replicationNodes =
                hashRing.getReplicationNodes(senderCoordinator);
        replicationNodes.add(senderCoordinator);

        senderCoordinator.setPrev(node);
        for (ECSNode replication : replicationNodes) {
            String[] deleteRange =
                    getOldLastReplication(replication).getNodeHashRange();
            result.add(new ECSDataTransferIssuer(replication, deleteRange));
        }

        return result;
    }

    private ECSNode getOldLastReplication(ECSNode node) {
        return hashRing.whoseLastReplication(node).getPrev();
    }

    public List<ECSDataTransferIssuer> removeNode(ECSNode node) {
        List<ECSDataTransferIssuer> result = new ArrayList<>();
        Collection<ECSNode> responsibleNodes = this.hashRing.getResponsibleNodes(node);

        hashRing.removeNode(node);

        // Each node holding full data range
        // No need to copy any other data
        if (hashRing.getSize() <= ECSHashRing.REPLICATION_NUM) {
            return result;
        }

        // handle data replica of node
        for (ECSNode coordinator : responsibleNodes) {
            result.add(new ECSDataTransferIssuer(coordinator,
                    hashRing.getLastReplication(coordinator), coordinator.getNodeHashRange()));
        }
        ECSNode nextNode = hashRing.getNextNode(node);
        result.add(new ECSDataTransferIssuer(nextNode,
                hashRing.getLastReplication(nextNode),
                new String[]{
                        nextNode.getPrev().getNodeHash(),
                        node.getNodeHash()
                }));

        return result.stream()
                .filter(Objects::nonNull).collect(Collectors.toList());
    }
}
