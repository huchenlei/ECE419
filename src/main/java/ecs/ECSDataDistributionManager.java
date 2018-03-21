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


    private ECSDataTransferIssuer transferFromCoordinator(
            ECSNode coordinator, ECSNode receiver, String[] hashRange) {
        ECSNode lastReplication = hashRing.getLastReplication(coordinator);
        if (lastReplication == null) {
            return new ECSDataTransferIssuer(
                    coordinator, receiver, hashRange,
                    ECSDataTransferIssuer.TransferType.DELETE
            );
        } else {
            return new ECSDataTransferIssuer(
                    lastReplication, receiver, hashRange,
                    ECSDataTransferIssuer.TransferType.COPY
            );
        }
    }

    private ECSDataTransferIssuer transferToReplica(
            ECSNode sender, ECSNode coordinator, String[] hashRange) {
        ECSNode lastReplication = hashRing.getLastReplication(coordinator);
        if (lastReplication == null
                || hashRing.getNextNode(lastReplication).equals(coordinator)) {
            // do not need to transfer data because there is no enough server
            return null;
        } else {
            return new ECSDataTransferIssuer(
                    sender, hashRing.getNextNode(lastReplication), hashRange,
                    ECSDataTransferIssuer.TransferType.COPY
            );
        }
    }

    public List<ECSDataTransferIssuer> addNode(ECSNode node) {
        List<ECSDataTransferIssuer> result = new ArrayList<>();
        ECSNode senderCoordinator = this.hashRing.getNodeByKey(node.getNodeHash());

        // Hash Ring empty
        if (senderCoordinator == null) return result;

        // handle coordinator data
        String[] hashRange = new String[]{
                senderCoordinator.getPrev().getNodeHash(),
                node.getNodeHash()
        };
        result.add(transferFromCoordinator(senderCoordinator, node, hashRange));

        // handle replication data
        Collection<ECSNode> responsibleNodes =
                hashRing.getResponsibleNodes(senderCoordinator);

        for (ECSNode coordinator : responsibleNodes) {
            result.add(transferFromCoordinator(
                    coordinator, node, coordinator.getNodeHashRange()));
        }

        return result;
    }

    public List<ECSDataTransferIssuer> removeNode(ECSNode node) {
        List<ECSDataTransferIssuer> result = new ArrayList<>();
        // handle data of node itself
        result.add(transferToReplica(node, node, node.getNodeHashRange()));

        // handle data replica of node
        for (ECSNode coordinator :
                hashRing.getResponsibleNodes(node)) {
            result.add(transferToReplica(node, coordinator, coordinator.getNodeHashRange()));
        }
        return result.stream()
                .filter(Objects::nonNull).collect(Collectors.toList());
    }
}
