package web_console;

public class ResponseWrapper {
    public String error = null;
    public Object result = null;

    public ResponseWrapper(String error, Object result) {
        this.error = error;
        this.result = result;
    }
}
