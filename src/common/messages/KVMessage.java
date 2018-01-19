package common.messages;

public interface KVMessage {
	
	public enum StatusType {
		GET, 			/* Get - request */
		GET_ERROR, 		/* requested tuple (i.e. value) not found */
		GET_SUCCESS, 	/* requested tuple (i.e. value) found */
		PUT, 			/* Put - request */
		PUT_SUCCESS, 	/* Put - request successful, tuple inserted */
		PUT_UPDATE, 	/* Put - request successful, i.e. value updated */
		PUT_ERROR, 		/* Put - request not successful */
		DELETE_SUCCESS, /* Delete - request successful */
		DELETE_ERROR, 	/* Delete - request successful */
		UNKNOWN_ERROR,  // Unknown error when something unknown happened
		BAD_STATUS_ERROR, // Unknown status code - when server received an unknown status
	}

	/**
	 * @return the key that is associated with this message, 
	 * 		null if not key is associated.
	 */
	public String getKey();
	
	/**
	 * @return the value that is associated with this message, 
	 * 		null if not value is associated.
	 */
	public String getValue();
	
	/**
	 * @return a status string that is used to identify request types, 
	 * response types and error types associated to the message.
	 */
	public StatusType getStatus();

	/**
	 * Setter of key
	 */
	public void setKey(String key);

	/**
	 * Setter of value
	 */
	public void setValue(String value);

	/**
	 * Setter of status
	 */
	public void setStatus(StatusType status);

	/**
	 * Encode the message object as a single string
	 * @return encoded string
	 */
	public String encode();

	/**
	 * Decode the input string as message object and save information
	 * in the message object calling decode method
	 * @param data input string to decode
	 */
	public void decode(String data);

}


