package performance;

import common.messages.JsonKVMessage;
import common.messages.KVMessage;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.DirectoryFileFilter;
import org.apache.commons.io.filefilter.RegexFileFilter;
import org.apache.log4j.Logger;

import java.io.*;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

/**
 * This class parses data in enron dataset to the format
 * of KV message
 */
public class DataParser {
    private static Logger logger = Logger.getRootLogger();
    public static final String DATA_ROOT = System.getProperty("user.home")
            + "/dataset/maildir";

    /**
     * Convert all data files under certain dir under DATA_ROOT
     * to KVMessage format
     *
     * @param dir dir under DATA_ROOT to search for
     * @return List of KVMessages
     */
    public static List<KVMessage> parseDataFrom(String dir) {
        Collection<File> files = FileUtils.listFiles(new File(DATA_ROOT + "/" + dir),
                new RegexFileFilter("^(.*?)"),
                DirectoryFileFilter.DIRECTORY);
        ArrayList<KVMessage> result = new ArrayList<>();
        for (File file : files) {
            try {
                StringBuilder val = new StringBuilder();
                BufferedReader reader = new BufferedReader(new FileReader(file));
                String line;
                while ((line = reader.readLine()) != null) {
                    val.append(line);
                }
                MessageDigest md = MessageDigest.getInstance("MD5");
                md.update(val.toString().getBytes());
                String key = new BigInteger(1, md.digest()).toString(16);
                result.add(new JsonKVMessage(key, val.toString(), "PUT"));
            } catch (FileNotFoundException e) {
                logger.error(file.getAbsolutePath() + " not found!");
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (NoSuchAlgorithmException e) {
                logger.fatal("Algorithm MD5 not found");
                logger.fatal(e.getMessage());
                e.printStackTrace();
            }
        }
        return result;
    }

    /**
     * Convert all data files under dirs under DATA_ROOT which start with
     * the starting char to KVMessage format
     *
     * @param startingChar the starting char of dir
     * @return List of KVMessages
     */
    public static List<KVMessage> parseDataFrom(Character startingChar) {
        File file = new File(DATA_ROOT);
        String[] dirs = file.list((dir, name) ->
                (name.startsWith(String.valueOf(startingChar)))
                        && (new File(dir, name).isDirectory()));

        ArrayList<KVMessage> result = new ArrayList<>();
        assert dirs != null;
        for (String dir : dirs) {
            assert parseDataFrom(dir) != null;
            result.addAll(Objects.requireNonNull(parseDataFrom(dir)));
        }
        return result;
    }
}
