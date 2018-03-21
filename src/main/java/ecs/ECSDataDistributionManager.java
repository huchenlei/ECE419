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

        // Hash Ring empty
        if (senderCoordinator == null) return result;

        // handle coordinator data
        String[] responsibleRange = this.hashRing.getResponsibleRange(senderCoordinator);
        String coordinatorNodeHash = senderCoordinator.getNodeHash();
        assert responsibleRange.length == 2;
        assert responsibleRange[1].equals(coordinatorNodeHash);
        String[] transferRange = new String[]{node.getNodeHash(), responsibleRange[0]};

        result.add(new ECSDataTransferIssuer(senderCoordinator, node,
                transferRange, ECSDataTransferIssuer.TransferType.COPY));

        // handle replication data (delete)
        Collection<ECSNode> responsibleNodes =
                hashRing.getResponsibleNodes(senderCoordinator);
        responsibleNodes.add(senderCoordinator);
        String[] deleteRange = new String[]{coordinatorNodeHash, node.getNodeHash()};
        for (ECSNode coordinator : responsibleNodes) {
            result.add(new ECSDataTransferIssuer(coordinator, node,
                    deleteRange, ECSDataTransferIssuer.TransferType.DELETE));
        }

        return result;
    }

    private ECSNode getNewLastReplication(ECSNode node) {
        return hashRing.getNextNode(hashRing.getLastReplication(node));
    }

    public List<ECSDataTransferIssuer> removeNode(ECSNode node) {
        List<ECSDataTransferIssuer> result = new ArrayList<>();

        // Each node holding full data range
        // No need to copy any other data
        if (hashRing.getSize() == ECSHashRing.REPLICATION_NUM + 1) {
            return result;
        }

        Collection<ECSNode> responsibleNodes = this.hashRing.getResponsibleNodes(node);

        // handle data replica of node
        for (ECSNode coordinator : responsibleNodes) {
            result.add(new ECSDataTransferIssuer(coordinator,
                    getNewLastReplication(coordinator), coordinator.getNodeHashRange(),
                    ECSDataTransferIssuer.TransferType.COPY));
        }
        result.add(new ECSDataTransferIssuer(hashRing.getNextNode(node),
                getNewLastReplication(node), node.getNodeHashRange(),
                ECSDataTransferIssuer.TransferType.COPY));

        return result.stream()
                .filter(Objects::nonNull).collect(Collectors.toList());
    }
}
