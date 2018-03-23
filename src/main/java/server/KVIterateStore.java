package server;

import ecs.ECSNode;
import org.apache.log4j.Logger;

import java.io.*;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiPredicate;

public class KVIterateStore implements KVPersistentStore {
    private String dir = "./res";
    private static Logger logger = Logger.getRootLogger();
    private String fileName = "iterateDatabase";
    private File storageFile;
    private String prompt = "KVIterateStore: ";
    public static String MOVE_SUFFIX = "_move";
    public static String REMAIN_SUFFIX = "_remain";

    private static final String ESCAPER = "-";
    private static final String DELIM = ESCAPER + ",";
    private static final String ESCAPED_ESCAPER = ESCAPER + "d";

    public static class KVEntry {
        long startOffset;
        long endOffset;
        String key = null;
        String value = null;


        public KVEntry(long startOffset, long endOffset) {
            this.startOffset = startOffset;
            this.endOffset = endOffset;
        }

        public KVEntry(long startOffset, long endOffset, String key, String value) {
            this(startOffset, endOffset);
            this.key = key;
            this.value = value;
        }

        public KVEntry(String key, String value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public String toString() {
            return "KVEntry{" +
                    "startOffset=" + startOffset +
                    ", endOffset=" + endOffset +
                    ", key='" + key + '\'' +
                    ", value='" + value + '\'' +
                    '}';
        }

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }
    }

    public KVIterateStore() {
        openFile();
    }

    public KVIterateStore(String fileName) {
        this.fileName = fileName;
        prompt = "KVIterateStore(" + fileName + "):";
        openFile();
    }

    public KVIterateStore(String fileName, String dir) {
        this.fileName = fileName;
        prompt = "KVIterateStore(" + fileName + "):";
        this.dir = dir;
        openFile();
    }

    private String encodeValue(String value) {
        return value.replaceAll("\r", "\\\\r")
                .replaceAll("\n", "\\\\n")
                .replaceAll(ESCAPER, ESCAPED_ESCAPER);
    }

    private String decodeValue(String value) {
        return value.replaceAll("\\\\r", "\r")
                .replaceAll("\\\\n", "\n")
                .replaceAll(ESCAPED_ESCAPER, ESCAPER);
    }

    private byte[] encodeLine(String key, String val) throws UnsupportedEncodingException {
        return (encodeValue(key) + DELIM + encodeValue(val) + "\r\n").getBytes("UTF-8");
    }

    public synchronized void deleteEntry(KVEntry entry) throws IOException {
        RandomAccessFile raf = new RandomAccessFile(this.storageFile, "rw");
        deleteEntry(raf, entry.startOffset, entry.endOffset);
        raf.close();
    }

    public synchronized void updateEntry(KVEntry entry) throws IOException {
        RandomAccessFile raf = new RandomAccessFile(this.storageFile, "rw");
        updateEntry(raf, entry.startOffset, entry.endOffset, encodeLine(entry.getKey(), entry.getValue()));
        raf.close();
    }

    public synchronized void appendEntry(KVEntry entry) throws IOException {
        RandomAccessFile raf = new RandomAccessFile(this.storageFile, "rw");
        appendEntry(raf, encodeLine(entry.getKey(), entry.getValue()));
        raf.close();
    }


    private void deleteEntry(RandomAccessFile raf, long offset1, long offset2) throws IOException {
        synchronized (this) {
            RandomAccessFile rTemp = new RandomAccessFile(new File(this.dir + "/" + "." + this.fileName + "~"),
                    "rw");
            long fileSize = raf.length();
            FileChannel sourceChannel = raf.getChannel();
            FileChannel targetChannel = rTemp.getChannel();
            // move rest of the content (from end of the entry) to temp channel
            sourceChannel.transferTo(offset2, fileSize - offset2, targetChannel);
            // remove content from the start of the entry
            sourceChannel.truncate(offset1);
            // move content back
            targetChannel.position(0L);
            sourceChannel.transferFrom(targetChannel, offset1, (fileSize - offset2));
            // clean the target_file
            targetChannel.truncate(0);
            sourceChannel.close();
            targetChannel.close();
            rTemp.close();
        }
    }

    private void updateEntry(RandomAccessFile raf, long offset1, long offset2, byte[] stringBytes) throws IOException {
        synchronized (this) {
            RandomAccessFile rTemp = new RandomAccessFile(new File(this.dir + "/" + "." + this.fileName + "~"),
                    "rw");
            long fileSize = raf.length();
            FileChannel sourceChannel = raf.getChannel();
            FileChannel targetChannel = rTemp.getChannel();
            // move rest of the content (from end of the entry) to temp channel
            sourceChannel.transferTo(offset2, fileSize - offset2, targetChannel);
            // remove content from the start of the entry
            sourceChannel.truncate(offset1);
            // insert the new entry
            raf.seek(offset1);
            raf.write(stringBytes);
            long newOffset = raf.getFilePointer();
            // move content back
            targetChannel.position(0L);
            sourceChannel.transferFrom(targetChannel, newOffset, (fileSize - offset2));
            // clean target channel
            targetChannel.truncate(0);
            sourceChannel.close();
            targetChannel.close();
            rTemp.close();
        }
    }

    private synchronized void appendEntry(RandomAccessFile raf, byte[] stringBytes) throws IOException {
        raf.seek(raf.length());
        raf.write(stringBytes);
    }


    @Override
    public void put(String key, String value) throws Exception {

        assert (this.storageFile != null);
        // search if key already exist;
        KVEntry entry = this._get(key);

        // construct an entry string with fixed length
        byte[] stringBytes = encodeLine(key, value);

        //modify the storage file
        RandomAccessFile raf = new RandomAccessFile(this.storageFile, "rw");
        try {
            if (value.equals("null")) {
                if (entry == null) {
                    logger.error(prompt + "Try to delete an entry with non-exist key: " + key);
                    throw new IOException("Try to delete an entry with non-exist key: " + key);
                } else {
                    // delete that entry
                    this.deleteEntry(raf, entry.startOffset, entry.endOffset);
                    logger.info(prompt + "Delete entry (" + key + "=" + entry.value + ") successfully");
                }
            } else if (entry == null) {
                // append the entry to the end
                appendEntry(raf, stringBytes);
                logger.info(prompt + "Insert new entry: (" + key + "=" + value + ") successfully");
            } else {
                this.updateEntry(raf, entry.startOffset, entry.endOffset, stringBytes);
                logger.info(prompt + "Modify entry with key: " + key + " (" + entry.value + "->" + value + ")");
            }

        } finally {
            raf.close();
        }
    }

    private KVEntry _get(String key) throws IOException {
        List<KVEntry> selected = select((k, v) -> k.equals(key));
        if (selected.size() > 1) {
            logger.error("Following duplicated key entry found:");
            selected.forEach(logger::error);

            boolean areSame = true;
            for (int i = 0; i < selected.size() - 1; i++) {
                areSame = selected.get(i).value.equals(selected.get(i + 1).value);
                if (!areSame) break;
            }

            if (areSame) {
                // Remove duplication
                // Fault tolerance
                synchronized (this) {
                    RandomAccessFile raf = new RandomAccessFile(this.storageFile, "rw");
                    for (int i = selected.size() - 1; i > 0; i--) {
                        KVEntry entry = selected.get(i);
                        deleteEntry(raf, entry.startOffset, entry.endOffset);
                        selected.remove(i);
                    }
                    raf.close();
                }
            }
        }
        assert selected.size() <= 1;
        return selected.size() == 1 ? selected.get(0) : null;
    }

    @Override
    public String get(String key) throws IOException {
        KVEntry entry = _get(key);
        return entry == null ? null : entry.value;
    }

    public List<KVEntry> select(BiPredicate<String, String> condition) throws IOException {
        synchronized (this) {
            assert (this.storageFile != null);
            List<KVEntry> result = new ArrayList<>();

            long endOffset = 0;
            long startOffset;
            try {
                RandomAccessFile raf = new RandomAccessFile(this.storageFile, "r");
                String line, curKey, curValue;
                while ((line = raf.readLine()) != null) {
                    // convert line from ISO to UTF-8
                    line = new String(line.getBytes("ISO-8859-1"), "UTF-8");
                    startOffset = endOffset;
                    endOffset = raf.getFilePointer();
                    if (line.isEmpty()) {
                        logger.fatal("how could it be");
                        continue;
                    }
                    String[] strs = line.split(DELIM);
                    if (strs.length != 2) {
                        raf.close();
                        throw new IOException(prompt + "Invalid Entry found when getting data: " + line);
                    }
                    curKey = decodeValue(strs[0]);
                    curValue = decodeValue(strs[1]);
                    if (condition.test(curKey, curValue)) {
                        result.add(new KVEntry(startOffset, endOffset, curKey, curValue));
                    }
                }
                raf.close();

            } catch (FileNotFoundException fnf) {
                logger.error(prompt + "Storage file not found", fnf);
                throw fnf;
            }

            return result;
        }
    }


    @Override
    public void clearStorage() {
        if (this.storageFile.delete()) {
            logger.info(prompt + "Storage file deleted successfully.");
        } else {
            logger.error(prompt + "Failed to delete storage file");
        }
        this.storageFile = null;
        openFile();
    }

    @Override
    public boolean inStorage(String key) throws Exception {
        String value = get(key);

        return (value != null);
    }

    @Override
    public String getfileName() {
        return this.dir + "/" + this.fileName;
    }

    public void deleteData(String[] hashRange) {
        synchronized (this) {
            File remainFile = new File(getfileName() + REMAIN_SUFFIX);
            try {
                remainFile.createNewFile();
                RandomAccessFile remainRaf = new RandomAccessFile(remainFile, "rw");
                RandomAccessFile raf = new RandomAccessFile(this.storageFile, "r");

                // insert remain data
                // read original file line by line
                String line, curKey, curValue;
                while ((line = raf.readLine()) != null) {
                    // convert line from ISO to UTF-8
                    line = new String(line.getBytes("ISO-8859-1"), "UTF-8");
                    if (line.isEmpty()) {
                        System.out.println("how could it be");
                        continue;
                    }
                    String[] strs = line.split(DELIM);
                    if (strs.length != 2) {
                        raf.close();
                        throw new IOException(prompt + "Invalid Entry found when deleting: " + line);
                    }
                    curKey = strs[0];
                    curValue = strs[1];
                    byte[] stringBytes = (curKey + DELIM + curValue + "\r\n").getBytes("UTF-8");
                    String oriKey = decodeValue(curKey);
                    // append to move file
                    if (!ECSNode.isKeyInRange(oriKey, hashRange)) {
                        long offset = remainRaf.length();
                        remainRaf.seek(offset);
                        remainRaf.write(stringBytes);
                    }
                }
                raf.close();
                remainRaf.close();

            } catch (IOException e) {
                logger.error(prompt + "Unable to remove file", e);
                e.printStackTrace();
            }

            // delete original file and rename the remain file
            if (this.storageFile.delete()) {
                logger.debug("Original file deleted");
            } else {
                logger.error("Unable to delete the original file");
            }
            if (remainFile.renameTo(new File(getfileName()))) {
                logger.debug("successfully rename the remain file");
            } else {
                logger.error("Unable to rename the remain file");
            }
        }

    }

    public void preMoveData(String[] hashRange) {
        synchronized (this) {
            File moveFile = new File(getfileName() + MOVE_SUFFIX);
            File remainFile = new File(getfileName() + REMAIN_SUFFIX);

            try {
                moveFile.createNewFile();
                remainFile.createNewFile();

                RandomAccessFile moveRaf = new RandomAccessFile(moveFile, "rw");
                RandomAccessFile remainRaf = new RandomAccessFile(remainFile, "rw");
                RandomAccessFile raf = new RandomAccessFile(this.storageFile, "r");

                // read original file line by line
                String line, curKey, curValue;
                while ((line = raf.readLine()) != null) {
                    // convert line from ISO to UTF-8
                    line = new String(line.getBytes("ISO-8859-1"), "UTF-8");
                    if (line.isEmpty()) {
                        System.out.println("how could it be");
                        continue;
                    }
                    String[] strs = line.split(DELIM);
                    if (strs.length != 2) {
                        raf.close();
                        throw new IOException(prompt + "Invalid Entry found when moving data: " + line);
                    }
                    curKey = strs[0];
                    curValue = strs[1];
                    byte[] stringBytes = (curKey + DELIM + curValue + "\r\n").getBytes("UTF-8");
                    String oriKey = decodeValue(curKey);
                    // append to move file
                    if (ECSNode.isKeyInRange(oriKey, hashRange)) {
                        long offset = moveRaf.length();
                        moveRaf.seek(offset);
                        moveRaf.write(stringBytes);
                    }
                    // append to remain file
                    else {
                        long offset = remainRaf.length();
                        remainRaf.seek(offset);
                        remainRaf.write(stringBytes);
                    }

                }
                raf.close();
                moveRaf.close();
                remainRaf.close();

            } catch (IOException e) {
                logger.error(prompt + "Unable to create move and remain file", e);
            }
        }

    }

    public void afterMoveData() {
        this.afterMoveData(true);
    }

    public void afterMoveData(boolean shouldDelete) {
        synchronized (this) {
            File moveFile = new File(getfileName() + MOVE_SUFFIX);
            File remainFile = new File(getfileName() + REMAIN_SUFFIX);

            if (moveFile.delete()) {
                logger.debug("Move file deleted");
            } else {
                logger.error("Unable to delete the move file");
            }

            // delete the original file and rename the remain file
            if (shouldDelete) {
                if (this.storageFile.delete()) {
                    logger.debug("Original file deleted");
                } else {
                    logger.error("Unable to delete the original file");
                }
                if (remainFile.renameTo(new File(getfileName()))) {
                    logger.debug("successfully rename the remain file");
                } else {
                    logger.error("Unable to rename the remain file");
                }
            } else {
                // delete the remain file
                if (remainFile.delete()) {
                    logger.debug("remain file deleted");
                } else {
                    logger.error("Unable to delete the remain file");
                }
            }

        }

    }

    private void openFile() {
        if (this.storageFile == null) {
            logger.info(prompt + "Initialize iterate storage file ...");
            boolean fileNotExist;
            File dir = new File(this.dir);
            if (!dir.exists()) {
                boolean mkdir_result = dir.mkdir();
                if (!mkdir_result) {
                    logger.error("Unable to create dir " + this.dir);
                    return;
                }
            }
            this.storageFile = new File(this.dir + "/" + this.fileName);
            try {
                fileNotExist = this.storageFile.createNewFile();
                if (fileNotExist) {
                    logger.info(prompt + "New storage file created");
                } else {
                    logger.info(prompt + "Storage file found");
                }
            } catch (IOException e) {
                logger.error(prompt + "Error when trying to initialize file instance", e);
            }
        }
    }

}
