package server;

import app_kvServer.IKVServer;
import org.apache.log4j.Logger;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

/**
 * Represents a connection end point for a particular client that is
 * connected to the server. This class is responsible for message reception
 * and sending.
 * <p>
 * After parsing the message, and getting its contents, will call KVServer to
 * perform corresponding actions.
 * <p>
 * Created by Charlie on 2018-01-12.
 */
public class KVClientConnection implements Runnable {
    private static Logger logger = Logger.getRootLogger();

    private boolean open;

    private IKVServer kvServer;
    private Socket clientSocket;
    private InputStream input;
    private OutputStream output;

    public boolean isOpen() {
        return open;
    }

    public KVClientConnection(IKVServer kvServer, Socket clientSocket) {
        this.kvServer = kvServer;
        this.clientSocket = clientSocket;
        this.open = true;
    }

    @Override
    public void run() {

    }
}
