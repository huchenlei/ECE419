package common.messages;

/**
 * The exception class dealing with message related exceptions
 * Created by Charlie on 2018-01-12.
 */
public class KVMessageException extends RuntimeException {
    public KVMessageException(String message) {
        super(message);
    }
}
