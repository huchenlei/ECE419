package app_kvClient;

import client.KVCommInterface;

public class KVClient implements IKVClient {
    @Override
    public void newConnection(String hostname, int port) throws Exception {

    }

    @Override
    public KVCommInterface getStore(){
        return null;
    }
}
