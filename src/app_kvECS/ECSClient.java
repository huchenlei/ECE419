package app_kvECS;

import app_kvClient.KVClient;
import client.ArgumentNumberException;
import ecs.ECS;
import ecs.IECSNode;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

public class ECSClient implements Runnable {
    private Logger logger = Logger.getRootLogger();
    private static final String PROMPT = "ecs> ";
    private BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in));
    private boolean running = false;
    private IECSClient ecs;

    public ECSClient(String configFileName) throws IOException {
        ecs = new ECS(configFileName);
    }

    private static Map<String, Integer> argTable = new HashMap<String, Integer>() {
        {
            put("start", 0);
            put("stop", 0);
            put("shutDown", 0);
            put("help", 0);
            put("addNode", 2);
            put("removeNodes", 1); // remove at least one node
            put("quit", 0);
        }
    };

    private void handleCommand(String cmdLine) {
        String[] tokens = cmdLine.split("\\s+");
        assert tokens.length > 0;
        String cmd = tokens[0];

        if (cmd == null) {
            System.out.println("Empty Command!");
            return;
        }

        try {
            KVClient.checkArgumentNum(tokens, argTable.get(cmd));
            boolean result = false;
            switch (cmd) {
                case "start":
                    result = ecs.start();
                    break;
                case "stop":
                    result = ecs.stop();
                    break;
                case "shutDown":
                    result = ecs.shutdown();
                    break;
                case "help":
                    printHelp();
                    break;
                case "addNode":
                    try {
                        IECSNode node = ecs.addNode(
                                tokens[1],
                                Integer.parseInt(tokens[2])
                        );
                        result = (node != null);
                    } catch (NumberFormatException nfe) {
                        printError("cache size must be an integer!\nUsage: addNode <strategy> <cacheSize>");
                        logger.info("Unable to parse argument <cacheSize>", nfe);
                    } catch (IllegalArgumentException iae) {
                        printError("Error! Invalid <strategy>! Must be one of [None LRU LFU FIFO]!");
                        logger.info("Unknown strategy", iae);
                    }
                    break;
                case "removeNodes":
                    List<String> serverNames = new ArrayList<>(Arrays.asList(tokens));
                    serverNames.remove(0); // remove cmd from head
                    result = ecs.removeNodes(serverNames);
                    break;
                case "quit":
                    this.running = false;
                    result = true;
                    break;
                default:
                    printError("Unknown command!");
                    printHelp();
            }
            if (result) {
                System.out.println(PROMPT + "operation successful");
            } else {
                System.out.println(PROMPT + "operation failed");
            }
        } catch (ArgumentNumberException e) {
            printError(e.getMessage());
        } catch (Exception e) {
            printError("Unknown error encountered!");
            e.printStackTrace();
        }
    }

    private void printError(String error) {
        System.out.println(PROMPT + "Error! " + error);
    }

    private void printHelp() {
        System.out.println("Usage: addNode/start/stop/shutDown/removeNodes/quit");
    }

    @Override
    public void run() {
        this.running = true;
        while (running) {
            System.out.print(PROMPT);
            try {
                String cmdLine = stdin.readLine();
                this.handleCommand(cmdLine);
            } catch (IOException e) {
                running = false;
                printError("ECS CLI does not respond - Application terminated " + e.getMessage());
            }
        }
    }

    public static void main(String[] args) {
        try {
            new LogSetup("logs/ecs.log", Level.ALL);
            if (args.length < 1) {
                System.err.println("Error! Invalid number of arguments!");
                System.err.println("Usage: ECS <config file>!");
            } else {
                new ECSClient(args[0]).run();
            }
        } catch (IOException e) {
            System.err.println("Unable to initialize logger!");
            e.printStackTrace();
            System.exit(1);
        }
    }
}
