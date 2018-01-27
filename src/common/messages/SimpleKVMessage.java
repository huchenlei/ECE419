package common.messages;

/**
 * A simple implementation of KVMessage.
 * Message is encoded in the format of
 * <p>
 * key-,value-,status
 * <p>
 * All '-' character in original data, i.e. key and value,
 * will be replaced by "-d" sequence
 * <p>
 * Created by Charlie on 2018-01-12.
 */
public class SimpleKVMessage extends AbstractKVMessage {
    /**
     * Since we use regex to match the delim in strings, delim itself can not have
     * specific meaning in regex (e.g. '|', '[', '(', etc can not be used as delim)
     * <p>
     * String in the message string will be joined by "-," sequence, while the original
     * "-" in message will be replaced by "-d" sequence
     */
    private static final String ESCAPER = "-";
    private static final String DELIM = ESCAPER + ",";
    private static final String ESCAPED_ESCAPER = ESCAPER + "d";

    public SimpleKVMessage() {
        super();
    }

    public SimpleKVMessage(String key, String value, StatusType status) {
        super(key, value, status);
    }

    public SimpleKVMessage(String key, String value, String status) {
        super(key, value, status);
    }

    public SimpleKVMessage(String data) {
        super(data);
    }

    // TODO to be removed after tester bug is fixed by TA
    @Override
    public String getKey() {
        return key;
    }

    @Override
    public String getValue() {
        return value;
    }

    @Override
    public StatusType getStatus() {
        return status;
    }

    @Override
    public String encode() {
        if (key == null
                || value == null
                || status == null) {
            throw new KVMessageException(
                    "Unable to encode message due to incomplete fields");
        }
        return String.join(DELIM,
                key.replaceAll(ESCAPER, ESCAPED_ESCAPER),
                value.replaceAll(ESCAPER, ESCAPED_ESCAPER),
                status.toString());
    }

    @Override
    public void decode(String data) {
        // Use look behind regex to split string
        // matches all DELIM that is not preceded with "--"
        String[] strs = data.trim().split(DELIM);

        if (strs.length != 3) {
            throw new KVMessageException(
                    "Mal-formatted packet: " + String.valueOf(strs.length) + " segments found\n"
                            + data);
        }

        key = strs[0].replaceAll(ESCAPED_ESCAPER, ESCAPER);
        value = strs[1].replaceAll(ESCAPED_ESCAPER, ESCAPER);
        status = StatusType.valueOf(StatusType.class, strs[2]);
    }
}
