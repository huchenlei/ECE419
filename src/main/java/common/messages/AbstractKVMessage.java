package common.messages;

import common.KVMessage;
import org.apache.log4j.Logger;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

/**
 * Base class of KV Message.
 * <p>
 * Provides the common fields used by most KVMessage implementations.
 * <p>
 * Created by Charlie on 2018-01-12.
 */
public abstract class AbstractKVMessage implements KVMessage {
    private static Logger logger = Logger.getRootLogger();

    /**
     * The default class of message to create
     */
    public static final Class<? extends KVMessage> defaultMessageClass = JsonKVMessage.class;

    /**
     * Current using class of message (used in both client and server side)
     * Change this to use different message protocols
     */
    public static Class<? extends KVMessage> currentMessageClass = defaultMessageClass;
    private static Constructor<?> currentConstructor;

    static {
        try {
            currentConstructor = currentMessageClass.getConstructor();
        } catch (NoSuchMethodException e) {
            logger.fatal("Default constructor not found in KVMessage implementation");
            e.printStackTrace();
        }
    }

    public static KVMessage createMessage() {
        try {
            return (KVMessage) currentConstructor.newInstance();
        } catch (InstantiationException |
                IllegalAccessException |
                InvocationTargetException e) {
            logger.fatal("Unable to create message with default constructor!");
            e.printStackTrace();
        }
        return null;
    }

    String key;
    String value;
    StatusType status;

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
    public void setKey(String key) {
        this.key = key;
    }

    @Override
    public void setValue(String value) {
        this.value = value;
    }

    @Override
    public void setStatus(StatusType status) {
        this.status = status;
    }

    public AbstractKVMessage() {
    }

    public AbstractKVMessage(String key, String value, StatusType status) {
        this();
        this.key = key;
        this.value = value;
        this.status = status;
    }

    public AbstractKVMessage(String key, String value, String status) {
        this(key, value, StatusType.valueOf(status));
    }

    public AbstractKVMessage(String data) {
        this();
        decode(data);
    }
}
