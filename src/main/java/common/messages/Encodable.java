package common.messages;

public interface Encodable {
    /**
     * Encode the message object as a single string
     *
     * @return encoded string
     */
    public String encode();
}
