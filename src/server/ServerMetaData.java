package server;

public class ServerMetaData {
    private String cacheStrategy;
    private Integer cacheSize;

    private Integer receivePort;
    private String host;
    /**
     * Between 0 and 100 (inclusive)
     * If no data transfer happening, the value should be 100 always
     */
    private Integer transferProgress;

    public ServerMetaData(String cacheStrategy, Integer cacheSize) {
        this.cacheStrategy = cacheStrategy;
        this.cacheSize = cacheSize;
        this.receivePort = 0;
        this.transferProgress = 100;
        this.host = "localhost-useless";
    }

    public Integer getReceivePort() {
        return receivePort;
    }

    public void setReceivePort(Integer receivePort) {
        this.receivePort = receivePort;
    }

    public void setCacheStrategy(String cacheStrategy) {
        this.cacheStrategy = cacheStrategy;
    }

    public void setCacheSize(Integer cacheSize) {
        this.cacheSize = cacheSize;
    }

    public String getCacheStrategy() {
        return cacheStrategy;
    }

    public Integer getCacheSize() {
        return cacheSize;
    }

    public Integer getTransferProgress() {
        return transferProgress;
    }

    public void setTransferProgress(Integer transferProgress) {
        this.transferProgress = transferProgress;
    }

    public boolean isIdle() {
        return transferProgress.equals(100);
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }
}
