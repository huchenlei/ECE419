package common;

import common.messages.Decodable;
import common.messages.Encodable;

public interface KVMessage extends Encodable, Decodable {
    enum StatusType {
        GET,            /* Get - request */
        GET_ERROR,        /* requested tuple (i.e. value) not found */
        GET_SUCCESS,    /* requested tuple (i.e. value) found */
        PUT,            /* Put - request */
        PUT_SUCCESS,    /* Put - request successful, tuple inserted */
        PUT_UPDATE,    /* Put - request successful, i.e. value updated */
        PUT_ERROR,        /* Put - request not successful */
        DELETE_SUCCESS, /* Delete - request successful */
        DELETE_ERROR,    /* Delete - request successful */
        UNKNOWN_ERROR,  // Unknown error when something unknown happened
        BAD_STATUS_ERROR, // Unknown status code - when server received an unknown status

        SERVER_STOPPED,         /* Server is stopped, no requests are processed */
        SERVER_WRITE_LOCK,      /* Server locked for out, only get possible */
        SERVER_NOT_RESPONSIBLE,  /* Request not successful, server not responsible for key */

        PUT_REPLICATE, // Replication put launched from other server

        SQL, // SQL request
        SQL_REPLICATE,
        SQL_ERROR,
        SQL_SUCCESS,
    }

    /**
     * @return the key that is associated with this message,
     * null if not key is associated.
     */
    String getKey();

    /**
     * @return the value that is associated with this message,
     * null if not value is associated.
     */
    String getValue();

    /**
     * @return a status string that is used to identify request types,
     * response types and error types associated to the message.
     */
    StatusType getStatus();

    /**
     * Setter of key
     */
    void setKey(String key);

    /**
     * Setter of value
     */
    void setValue(String value);

    /**
     * Setter of status
     */
    void setStatus(StatusType status);

}


