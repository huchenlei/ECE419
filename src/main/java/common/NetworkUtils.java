package common;

import org.apache.log4j.Logger;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class NetworkUtils {
    private static Logger logger = Logger.getRootLogger();

    public static String getCurrentHost() {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            logger.warn("Unable to get host address, setting host as localhost(127.0.0.1)");
            logger.fatal(e.getMessage());
            logger.fatal(e.getStackTrace());
            return "127.0.0.1";
        }
    }
}
