package common.connection;

import common.messages.TextMessage;
import org.apache.log4j.Logger;

//import javax.xml.soap.Text;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

/**
 * Abstract layer of connection object for both server and client side
 * Created by Charlie on 2018-01-12.
 */
public abstract class AbstractKVConnection implements KVConnection {
    protected boolean open;
    protected Socket clientSocket;
    protected InputStream input;
    protected OutputStream output;

    protected static Logger logger = Logger.getRootLogger();

    public boolean isOpen() {
        return open;
    }

    private String prompt = "NO-NAME";

    public void setPrompt(String prompt) {
        this.prompt = prompt;
    }

    public void disconnect() {
        try {
            if (clientSocket != null) {
                if (input != null)
                    input.close();
                if (output != null)
                    output.close();
                clientSocket.close();
            }
        } catch (IOException e) {
            logger.error("Unable to tear down connection!", e);
        }
    }

    @Override
    public void sendMessage(TextMessage msg) throws IOException {
        byte[] msgBytes = msg.getMsgBytes();
        output.write(msgBytes, 0, msgBytes.length);
        output.flush();
    }

    @Override
    public TextMessage receiveMessage() throws IOException {
        // Read the header of message to determine the length
        byte[] lenBuf = new byte[TextMessage.LEN_DIGIT];
        if (input.read(lenBuf, 0, TextMessage.LEN_DIGIT) != TextMessage.LEN_DIGIT) {
            throw new IOException("Invalid message format, can not read the length of packet!");
        }
        Integer len = Integer.parseInt(new String(lenBuf));

        byte[] msgBytes = new byte[len];
        if (input.read(msgBytes, 0, len) != len) {
            throw new IOException("Invalid message length, more bytes expected");
        }

        if (input.read() != 0x0A ||
                input.read() != 0x0D) {
            throw new IOException("Expecting CR, LF sequence at the end of packet");
        }

        /* build final String */
        TextMessage msg = new TextMessage(msgBytes);

        //handle the empty input issue, happened when disconnect without sending KVMessage
        if (msg.getMsg().matches("[\\n\\r]+")) {
            throw new IOException("Received an empty message");
        }
        logger.info("(" + prompt + ")RECEIVE <"
                + clientSocket.getInetAddress().getHostAddress() + ":"
                + clientSocket.getPort() + ">: '"
                + msg.getMsg().trim() + "'");
        return msg;
    }
}
