package common.messages;

/**
 * A simple implementation of KVMessage.
 * Message is encoded in the format of
 * <p>
 * key,value,status
 * <p>
 * All ',' character in original data, i.e. key and value,
 * will be escaped by --," sequence
 * <p>
 * Created by Charlie on 2018-01-12.
 */
public class SimpleKVMessage extends AbstractKVMessage {
    /**
     * Since we use regex to match the delim in strings, delim itself can not have
     * specific meaning in regex (e.g. '|', '[', '(', etc can not be used as delim)
     */
    private static final String DELIM = ",";
    private static final String ESCAPED_DELIM = "--" + DELIM;

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

    @Override
    public String encode() {
        if (key == null
                || value == null
                || status == null) {
            throw new KVMessageException(
                    "Unable to encode message due to incomplete fields");
        }
        return String.join(DELIM,
                key.replaceAll(DELIM, ESCAPED_DELIM),
                value.replaceAll(DELIM, ESCAPED_DELIM),
                status.toString());
    }

    @Override
    public void decode(String data) throws KVMessageException {
        // Use look behind regex to split string
        // matches all DELIM that is not preceded with "--"
        String[] strs = data.trim().split("(?<!--)" + DELIM);

        if (strs.length != 3) {
            throw new KVMessageException(
                    "Mal-formatted packet: " + String.valueOf(strs.length) + " segments found\n"
                            + data);
        }

        key = strs[0].replaceAll(ESCAPED_DELIM, DELIM);
        value = strs[1].replaceAll(ESCAPED_DELIM, DELIM);
        status = StatusType.valueOf(StatusType.class, strs[2]);
    }

    public static void main(String[] args) {
        String[] strs = "a simple,,Something".split(",");
        System.out.println(strs.length);
        for (String str: strs) {
            if (str != null)
                System.out.println(str);
        }
    }
}
