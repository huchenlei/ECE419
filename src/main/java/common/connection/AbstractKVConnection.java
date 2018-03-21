package common.connection;

import common.messages.TextMessage;
import org.apache.log4j.Logger;
import org.apache.commons.codec.binary.Hex;

import java.io.*;
import java.net.Socket;
import java.util.Objects;

/**
 * Abstract layer of connection object for both server and client side
 * Created by Charlie on 2018-01-12.
 */
public abstract class AbstractKVConnection implements KVConnection {
    protected String address;
    protected int port;
    protected boolean open;
    protected Socket clientSocket;
    protected BufferedInputStream input;
    protected BufferedOutputStream output;

    protected static Logger logger = Logger.getRootLogger();

    public boolean isOpen() {
        return open;
    }

    private String prompt = "NO-NAME";

    public void setPrompt(String prompt) {
        this.prompt = prompt;
    }

    public void connect() throws IOException {
        this.clientSocket = new Socket(address, port);
        this.input = new BufferedInputStream(clientSocket.getInputStream());
        this.output = new BufferedOutputStream(clientSocket.getOutputStream());
    }

    public void disconnect() {
        try {
            if (clientSocket != null) {
                this.input.close();
                this.output.close();
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
        int read_len;
        int max_wait = 5;
        int wait_count = 0;
        while ((read_len = input.read(lenBuf, 0, TextMessage.LEN_DIGIT))
                != TextMessage.LEN_DIGIT) {
            if (read_len == -1 && wait_count++ < max_wait) {
                continue;
            } else if (wait_count++ < max_wait){
                logger.error("Message header(len " + read_len + ") incomplete " + Hex.encodeHexString(lenBuf));
                continue;
            }
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AbstractKVConnection that = (AbstractKVConnection) o;
        return port == that.port &&
                Objects.equals(address, that.address);
    }

    @Override
    public int hashCode() {
        return Objects.hash(address, port);
    }
}
