package server;

import java.io.*;
import java.nio.channels.FileChannel;

import org.apache.log4j.Logger;

public class KVIterateStore implements KVPersistentStore {
    private String dir = "./res";
    private static Logger logger = Logger.getRootLogger();
    private String fileName = "iterateDatabase";
    private File storageFile;
    private String prompt = "KVIterateStore: ";
    private long startOffset;
    private long endOffset;

    public KVIterateStore() {
        openFile();
    }

    public KVIterateStore(String fileName) {
        this.fileName = fileName;
        openFile();
    }

    public KVIterateStore(String fileName, String dir) {
        this.fileName = fileName;
        this.dir = dir;
        openFile();
    }

    private String encodeValue(String value) {
        return value.replaceAll("\r", "\\\\r").replaceAll("\n", "\\\\n");
    }
    private String decodeValue(String value) {
        return value.replaceAll("\\\\r", "\r").replaceAll("\\\\n", "\n");
    }

    private void deleteEntry(RandomAccessFile raf, long offset1, long offset2) throws IOException {
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

    private void updateEntry(RandomAccessFile raf, long offset1, long offset2, byte[] stringBytes) throws IOException{
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


    @Override
    public void put(String key, String value) throws Exception {
        String value_store = encodeValue(value);

        assert (this.storageFile != null);
        // search if key already exist;
        String getValue = this.get(key);

        // construct an entry string with fixed length
        byte[] stringBytes = (key + "=" + value_store + "\r\n").getBytes("UTF-8");

        //modify the storage file
        RandomAccessFile raf = new RandomAccessFile(this.storageFile, "rw");
        try {
            if (value_store.equals("null")) {
                if (getValue == null) {
                    logger.error(prompt + "Try to delete an entry with non-exist key: " + key);
                } else {
                    // delete that entry
                    this.deleteEntry(raf, this.startOffset, this.endOffset);
                    logger.info(prompt + "Delete entry (" + key + "=" + getValue + ") successfully");
                }
            } else if (getValue == null) {
                // append the entry to the end
                long offset = raf.length();
                raf.seek(offset);
                raf.write(stringBytes);
                logger.info(prompt + "Insert new entry: (" + key + "=" + value_store + ") successfully");
            } else {
                this.updateEntry(raf, this.startOffset, this.endOffset, stringBytes);
                logger.info(prompt + "Modify entry with key: " + key + " (" + getValue + "->" + value_store + ")");
            }

        } finally {
            raf.close();
        }

    }

    @Override
    public String get(String key) throws Exception {
        assert (this.storageFile != null);
        String value = null;
        this.startOffset = 0;
        this.endOffset = 0;
        try {
            RandomAccessFile raf = new RandomAccessFile(this.storageFile, "r");
            String line, curKey, curValue;
            while ((line = raf.readLine()) != null) {
                // convert line from ISO to UTF-8
                line = new String(line.getBytes("ISO-8859-1"), "UTF-8");
                this.startOffset = this.endOffset;
                this.endOffset = raf.getFilePointer();
                line = line.trim();
                if (line.isEmpty()) {
                    System.out.println("how could it be");
                    continue;
                }
                String[] strs = line.split("=");
                if (strs.length != 2) {
                    raf.close();
                    throw new IOException(prompt + "Invalid Entry found: " + line);
                }
                curKey = strs[0];
                curValue = strs[1];

                if (curKey.equals(key)) {
                    value = curValue;
                    break;
                }
            }
            raf.close();

        } catch (FileNotFoundException fnf) {
            logger.error(prompt + "Storage file not found", fnf);
            throw fnf;
        }

        if (value != null){
            value = decodeValue(value);
        }
        return value;
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

    private void openFile() {
        if (this.storageFile == null) {
            logger.info(prompt + "Initialize iterate storage file ...");
            boolean fileNotExist;
            File dir = new File(this.dir);
            if (!dir.exists()) {
                boolean mkdir_result = dir.mkdir();
                if (!mkdir_result) {
                    logger.error("Unable to create dir " + this.dir);
                    // TODO throw corresponding exception
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
