package client;

/**
 * Created by Charlie on 2018-01-13.
 */
public class ArgumentNumberException extends Exception {
    public ArgumentNumberException(int expected, int got) {
        super("Expecting " + expected + " arguments, but got " + got + ".");
    }
}
