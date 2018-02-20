package common.messages;

public interface Decodable {
    /**
     * Decode the input string as message object and save information
     * in the message object calling decode method
     *
     * @param data input string to decode
     */
    public void decode(String data);
}
