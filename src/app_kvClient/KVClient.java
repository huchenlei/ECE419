package app_kvClient;

import client.ArgumentNumberException;
import client.KVCommInterface;
import client.KVStore;
import common.KVMessage;
import common.messages.KVMessageException;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.UnknownHostException;

public class KVClient implements IKVClient, Runnable {
    private static Logger logger = Logger.getRootLogger();
    private static final String PROMPT = "> ";
    private BufferedReader stdin;
    private KVCommInterface client;

    private boolean running;

    @Override
    public void newConnection(String hostname, int port) throws Exception {
        if (this.client != null) {
            throw new IOException("Connection is already established");
        }
        this.client = new KVStore(hostname, port);
        this.client.connect();
    }

    @Override
    public KVCommInterface getStore() {
        return client;
    }

    /**
     * The key and values are assumed to be not containing any empty spaces(\s)
     *
     * @param cmdLine command line input string from user
     */
    private void handleCommand(String cmdLine) {
        String[] tokens = cmdLine.split("\\s+");
        String cmd = tokens[0];
        try {
            if (cmd.equals("quit") || cmd.equals("q")) {
                checkArgumentNum(tokens, 1);
                running = false;
                disconnect();
                System.out.println(PROMPT + "Application Exit!");
            } else if (cmd.equals("connect") || cmd.equals("c")) {
                try {
                    checkArgumentNum(tokens, 3);
                    newConnection(
                            tokens[1],
                            Integer.parseInt(tokens[2])
                    );
                    System.out.println(PROMPT + "Connected to server " + tokens[1] + ":" + tokens[2]);
                } catch (NumberFormatException nfe) {
                    printError("No valid address. Port must be a number!");
                    logger.info("Unable to parse argument <port>", nfe);
                } catch (UnknownHostException e) {
                    printError("Unknown Host!");
                    logger.info("Unknown Host!", e);
                } catch (IOException e) {
                    printError("Could not establish connection!");
                    logger.warn("Could not establish connection!", e);
                }
            } else if (cmd.equals("disconnect") || cmd.equals("d")) {
                checkArgumentNum(tokens, 1);
                disconnect();
                System.out.println(PROMPT + "Connection terminated.");

            } else if (cmd.equals("get")) {
                checkArgumentNum(tokens, 2);
                KVMessage res = client.get(tokens[1]);
                if (res != null) {
                		printMessage(res);
                }
            } else if (cmd.equals("put")) {
                checkArgumentNum(tokens, 3);
                KVMessage res = client.put(tokens[1], tokens[2]);
                if (res != null) {
            			printMessage(res);
                }
            } else if (cmd.equals("logLevel")) {
                checkArgumentNum(tokens, 2);
                String level = setLevel(tokens[1]);
                if (level.equals(LogSetup.UNKNOWN_LEVEL)) {
                    printError("No valid log level!");
                    printPossibleLogLevels();
                } else {
                    System.out.println(PROMPT +
                            "Log level changed to level " + level);
                }
            } else if (cmd.equals("help")) {
                printHelp();
            } else {
                printError("Unknown command!");
                printHelp();
            }

        } catch (ArgumentNumberException e) {
            printError(e.getMessage());
        } catch (KVMessageException kvme) {
            printError("Message related error encountered: " + kvme.getMessage());
        } catch (Exception e) {
            printError("Unknown error encountered!");
            e.printStackTrace();
        }
    }

    public void disconnect() {
        if (client != null) {
            client.disconnect();
            client = null;
        }
    }

    private String setLevel(String levelString) {
        if (levelString.equals(Level.ALL.toString())) {
            logger.setLevel(Level.ALL);
            return Level.ALL.toString();
        } else if (levelString.equals(Level.DEBUG.toString())) {
            logger.setLevel(Level.DEBUG);
            return Level.DEBUG.toString();
        } else if (levelString.equals(Level.INFO.toString())) {
            logger.setLevel(Level.INFO);
            return Level.INFO.toString();
        } else if (levelString.equals(Level.WARN.toString())) {
            logger.setLevel(Level.WARN);
            return Level.WARN.toString();
        } else if (levelString.equals(Level.ERROR.toString())) {
            logger.setLevel(Level.ERROR);
            return Level.ERROR.toString();
        } else if (levelString.equals(Level.FATAL.toString())) {
            logger.setLevel(Level.FATAL);
            return Level.FATAL.toString();
        } else if (levelString.equals(Level.OFF.toString())) {
            logger.setLevel(Level.OFF);
            return Level.OFF.toString();
        } else {
            return LogSetup.UNKNOWN_LEVEL;
        }
    }

    private void printPossibleLogLevels() {
        System.out.println(PROMPT
                + "Possible log levels are:");
        System.out.println(PROMPT
                + "ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF");
    }

    public static void checkArgumentNum(String[] tokens, int expected)
            throws ArgumentNumberException {
        if (tokens.length < expected) {
            throw new ArgumentNumberException(expected, tokens.length);
        }
    }

    private void printHelp() {
        StringBuilder sb = new StringBuilder();
        sb.append(PROMPT).append("\n");
        sb.append(PROMPT).append("KVClient Help (Usage):\n");
        sb.append(PROMPT);
        sb.append("::::::::::::::::::::::::::::::::");
        sb.append("::::::::::::::::::::::::::::::::\n");
        sb.append(PROMPT).append("connect <host> <port>");
        sb.append("\t\t establishes a connection to a server\n");
        sb.append(PROMPT).append("disconnect");
        sb.append("\t\t\t disconnects from the server \n");
        sb.append(PROMPT).append("put <key> <value>");
        sb.append("\t\t inserts/updates a key-value pair into the storage \n");
        sb.append(PROMPT).append("put <key> null");
        sb.append("\t\t deletes the entry for the given key  \n");
        sb.append(PROMPT).append("get <key>");
        sb.append("\t\t\t retrieves the value for the given key \n");
        sb.append(PROMPT).append("logLevel <level>");
        sb.append("\t\t changes the logLevel \n");
        sb.append(PROMPT).append("\t\t\t\t ");
        sb.append("ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF \n");
        sb.append(PROMPT).append("quit ");
        sb.append("\t\t\t\t exits the program \n");
        sb.append(PROMPT);
        sb.append("::::::::::::::::::::::::::::::::");
        sb.append("::::::::::::::::::::::::::::::::");
        System.out.println(sb.toString());
    }

    private void printMessage(KVMessage m) {
        System.out.println(PROMPT + m.getStatus() + ": <" + m.getKey() + ", " + m.getValue() + ">");
    }

    private void printError(String error) {
        System.out.println(PROMPT + "Error! " + error);
    }

    @Override
    public void run() {
        this.running = true;
        while (running) {
            stdin = new BufferedReader(new InputStreamReader(System.in));
            System.out.print(PROMPT);

            try {
                String cmdLine = stdin.readLine();
                this.handleCommand(cmdLine);
            } catch (IOException e) {
                running = false;
                printError("CLI does not respond - Application terminated ");
            }
        }
    }

    public static void main(String[] args) {
        try {
            new LogSetup("logs/client.log", Level.ALL);
            new KVClient().run();
        } catch (IOException e) {
            System.err.println("Unable to initialize logger!");
            e.printStackTrace();
            System.exit(1);
        }
    }
}
