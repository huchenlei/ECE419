package common.connection;

import common.messages.TextMessage;

import java.io.IOException;

/**
 * The connection interface for both server side and client side connection
 * representation
 * Created by Charlie on 2018-01-12.
 */
public interface KVConnection {
    /**
     * Method sends a TextMessage using this socket.
     *
     * @param msg the message that is to be sent.
     * @throws IOException some I/O error regarding the output stream
     */
    public void sendMessage(TextMessage msg) throws IOException;

    public TextMessage receiveMessage() throws IOException;
}
