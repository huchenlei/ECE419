package server;

import app_kvServer.IKVServer;
import app_kvServer.KVServer;
import org.apache.log4j.Logger;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.channels.FileChannel;

public class KVServerReceiver implements Runnable {
    private ServerSocket receiverSocket;
    private IKVServer kvServer;
    protected static Logger logger = Logger.getRootLogger();

    public KVServerReceiver(IKVServer kvServer, ServerSocket receiverSocket){
        this.kvServer = kvServer;
        this.receiverSocket = receiverSocket;
    }

    @Override
    public void run() {
        try {
            Socket client = receiverSocket.accept();

            byte[] buffer = new byte[KVServer.BUFFER_SIZE];

            String fileName = kvServer.getStorageName();
            BufferedInputStream in = new BufferedInputStream(client.getInputStream());
            BufferedOutputStream out = new BufferedOutputStream(
                    new FileOutputStream(fileName + "~"));

            int len = 0;
            while ((len = in.read(buffer)) > 0) {
                out.write(buffer, 0, len);
            }


            logger.info("Connected to Other Server"
                    + client.getInetAddress().getHostName()
                    + " on port " + client.getPort());

            in.close();
            out.flush();
            out.close();
            client.close();
            receiverSocket.close();

            // merge two files
            File tempFile = new File(fileName + "~");
            RandomAccessFile rTemp = new RandomAccessFile(tempFile, "rw");
            RandomAccessFile raf = new RandomAccessFile(new File(fileName), "rw");
            long sourceFileSize = raf.length();
            long targetFileSize = rTemp.length();
            FileChannel sourceChannel = raf.getChannel();
            FileChannel targetChannel = rTemp.getChannel();

            targetChannel.position(0L);
            sourceChannel.transferFrom(targetChannel, sourceFileSize, targetFileSize);
            // clean the target_file
            targetChannel.truncate(0);
            sourceChannel.close();
            targetChannel.close();
            rTemp.close();
            raf.close();
            tempFile.delete();

            // update the progress to 100
            ((KVServer)this.kvServer).updateTransferProgress(100);
            logger.debug("Receiver Finish receiving");

            // release the lock
            kvServer.unlockWrite();

        } catch (IOException e) {
            logger.error("Unable to establish connection with client.\n", e);
        }

    }

}
