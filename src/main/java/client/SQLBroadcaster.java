package client;

import common.connection.AbstractKVConnection;
import ecs.ECSHashRing;

public class SQLBroadcaster {
    private ECSHashRing hashRing;

    public class SQLConnection extends AbstractKVConnection {

    }

    public SQLBroadcaster(ECSHashRing hashRing) {
        this.hashRing = hashRing;
    }

    public void setHashRing(ECSHashRing hashRing) {
        this.hashRing = hashRing;
    }
}
