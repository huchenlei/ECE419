package common.messages;

import java.io.Serializable;

/**
 * Represents a simple text message, which is intended to be received and sent
 * by the server.
 */
public class TextMessage implements Serializable {

    private static final long serialVersionUID = 5549512212003782618L;
    private String msg;
    private byte[] msgBytes;
    private static final char LINE_FEED = 0x0A;
    private static final char RETURN = 0x0D;

    /**
     * Constructs a TextMessage object with a given array of bytes that
     * forms the message.
     *
     * @param bytes the bytes that form the message in ASCII coding.
     */
    public TextMessage(byte[] bytes) {
        this.msgBytes = addCtrChars(bytes);
        this.msg = new String(bytes);
    }

    /**
     * Constructs a TextMessage object with a given String that
     * forms the message.
     *
     * @param msg the String that forms the message.
     */
    public TextMessage(String msg) {
        this.msg = msg;
        this.msgBytes = toByteArray(msg);
    }


    /**
     * Returns the content of this TextMessage as a String.
     *
     * @return the content of this message in String format.
     */
    public String getMsg() {
        return msg;
    }

    /**
     * Returns an array of bytes that represent the ASCII coded message content.
     *
     * @return the content of this message as an array of bytes
     * in ASCII coding.
     */
    public byte[] getMsgBytes() {
        return msgBytes;
    }

    public static final Integer LEN_DIGIT = 16;

    // Pad an 16 digit(byte) number at the beginning of each message for the length
    // of the message excluding the end CR, LF sequence
    // Add the CR, LF sequence at the end of each message
    private byte[] addCtrChars(byte[] bytes) {
        byte[] ctrBytes = new byte[]{LINE_FEED, RETURN};
        byte[] lenBytes = String.format("%0" + LEN_DIGIT + "d", bytes.length).getBytes();
        byte[] tmp = new byte[bytes.length + ctrBytes.length + LEN_DIGIT];

        // Avoid overflow
        assert ctrBytes.length < Math.pow(10, LEN_DIGIT);

        System.arraycopy(lenBytes, 0, tmp, 0, LEN_DIGIT);
        System.arraycopy(bytes, 0, tmp, LEN_DIGIT, bytes.length);
        System.arraycopy(ctrBytes, 0, tmp, LEN_DIGIT + bytes.length, ctrBytes.length);

        return tmp;
    }

    private byte[] toByteArray(String s) {
        return addCtrChars(s.getBytes());
    }

}
