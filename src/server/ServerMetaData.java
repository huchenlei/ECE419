package server;

public class ServerMetaData {
    String cacheStrategy;
    Integer cacheSize;

    public ServerMetaData(String cacheStrategy, Integer cacheSize) {
        this.cacheStrategy = cacheStrategy;
        this.cacheSize = cacheSize;
    }
}
