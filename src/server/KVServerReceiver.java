package server;

import app_kvServer.IKVServer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class KVServerReceiver implements Runnable{
    private ServerSocket receiverSocket;
    private IKVServer kvServer;
    protected static Logger logger = Logger.getRootLogger();

    public KVServerReceiver(IKVServer kvServer, ServerSocket receiverSocket){
        this.kvServer = kvServer;
        this.receiverSocket = receiverSocket;
    }

    @Override
    public void run() {
        try {
            Socket client = receiverSocket.accept();

            // TODO: handle data transfer

            logger.info("Connected to Other Server"
                    + client.getInetAddress().getHostName()
                    + " on port " + client.getPort());

            // release the lock
            kvServer.unlockWrite();

        } catch (IOException e) {
            logger.error("Unable to establish connection with client.\n", e);
        }

    }

}
