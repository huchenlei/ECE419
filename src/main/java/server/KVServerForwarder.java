package server;

import common.connection.AbstractKVConnection;
import common.messages.AbstractKVMessage;
import common.KVMessage;
import common.messages.TextMessage;
import ecs.ECSNode;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * This class will forward the put requests from coordinator server to
 * replication servers
 */
public class KVServerForwarder extends AbstractKVConnection {
    public static final List<KVMessage.StatusType> successStatus = Arrays.asList(
            KVMessage.StatusType.PUT_SUCCESS,
            KVMessage.StatusType.PUT_UPDATE,
            KVMessage.StatusType.DELETE_SUCCESS
    );

    public KVServerForwarder(ECSNode node) {
        assert node != null;
        this.address = node.getNodeHost();
        this.port = node.getNodePort();
    }

    public void forward(KVMessage message) throws IOException, ForwardFailedException {
        if (!message.getStatus().equals(KVMessage.StatusType.PUT))
            throw new ForwardFailedException("Must forward put request! but get "
                    + message.getStatus());

        KVMessage req = AbstractKVMessage.createMessage();
        KVMessage res = AbstractKVMessage.createMessage();

        assert req != null;
        assert res != null;
        req.setKey(message.getKey());
        req.setStatus(KVMessage.StatusType.PUT_REPLICATE);
        req.setValue(message.getValue());

        sendMessage(new TextMessage(req.encode()));
        res.decode(receiveMessage().getMsg());

        if (!successStatus.contains(res.getStatus())) {
            throw new ForwardFailedException(
                    "Forward to server at " + this.address + ":" + this.port + " failed " + res);
        }
    }

    public static class ForwardFailedException extends Exception {
        public ForwardFailedException(String msg) {
            super(msg);
        }
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }
}
