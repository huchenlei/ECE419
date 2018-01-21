package server;

import java.io.*;

import org.apache.log4j.Logger;

public class KVIterateStore implements KVPersistentStore {
    private String dir = "./res";
    private static Logger logger = Logger.getRootLogger();
    private String fileName = "iterateDatabase";
    private File storageFile;
    private long lineNum;
    private long firstEmptyLine;
    private int entrySize = 200;
    private String deleteSymbol = "Deleted";
    private String prompt = "KVIterateStore: ";

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

    @Override
    public void put(String key, String value) throws Exception {
        assert (this.storageFile != null);

        // search if key already exist;
        String getValue = this.get(key);

        // construct an entry string with fixed length
        byte[] stringBytes = new String(key + "=" + value + "\n").getBytes("UTF-8");
        byte[] entryBytes = new byte[entrySize];
        System.arraycopy(stringBytes, 0, entryBytes, 0, stringBytes.length);

        //modify the storage file
        RandomAccessFile raf = new RandomAccessFile(this.storageFile, "rw");
        try {
            if (value.equals("null")) {
                if (getValue == null) {
                    logger.error(prompt + "Try to delete an entry with non-exist key: " + key);
                } else {
                    // empty line can not be recognized by getter, so change value to null instead.
                    raf.seek((this.lineNum - 1) * entrySize);
                    raf.write(entryBytes);
                    logger.info(prompt + "Delete entry (" + key + "=" + getValue + ") successfully");
                }
            } else if (getValue == null) {

                // append the entry to the first empty space;
                long offset = 0;
                if (this.firstEmptyLine == -1) {
                    offset = this.storageFile.length();
                } else {
                    offset = (this.firstEmptyLine - 1) * entrySize;
                }
                raf.seek(offset);
                raf.write(entryBytes);
                logger.info(prompt + "Insert new entry: (" + key + "=" + value + ") successfully");
            } else {
                raf.seek((this.lineNum - 1) * entrySize);
                raf.write(entryBytes);
                logger.info(prompt + "Modify entry with key: " + key + " (" + getValue + "->" + value + ")");
            }

        } finally {
            raf.close();
        }

    }

    @Override
    public String get(String key) throws Exception {
        assert (this.storageFile != null);
        String value = null;
        long linecount = 0;
        this.lineNum = 0;
        this.firstEmptyLine = -1;
        try {
            BufferedReader bufReader = new BufferedReader(new FileReader(this.storageFile));
            String line, curKey, curValue;
            while ((line = bufReader.readLine()) != null) {
                linecount++;
                line = line.trim();
                if (line.isEmpty()) {
                    if (this.firstEmptyLine == -1) {
                        this.firstEmptyLine = linecount;
                    }
                    continue;
                }
                String[] strs = line.split("=");
                if (strs.length != 2) {
                    throw new IOException(prompt + "Invalid Entry found: " + line);
                }
                curKey = strs[0];
                curValue = strs[1];

                // if value = null, treat it as empty line
                if (curValue.equals("null")) {
                    if (this.firstEmptyLine == -1) {
                        this.firstEmptyLine = linecount;
                    }
                } else if (curKey.equals(key)) {
                    value = curValue;
                    this.lineNum = linecount;
                    break;
                }
            }
            bufReader.close();

        } catch (FileNotFoundException fnf) {
            logger.error(prompt + "Storage file not found", fnf);
            throw fnf;
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
