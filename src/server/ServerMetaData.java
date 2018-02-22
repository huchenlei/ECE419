package server;

public class ServerMetaData {
    String cacheStrategy;
    Integer cacheSize;
    public ServerMetaData(String cacheStrategy, Integer cacheSize) {
        this.cacheStrategy = cacheStrategy;
        this.cacheSize = cacheSize;
    }

    public String getCacheStrategy() {
        return cacheStrategy;
    }

    public Integer getCacheSize() {
        return cacheSize;
    }
}
