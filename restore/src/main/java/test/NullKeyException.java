package test;

public class NullKeyException extends Exception{
    public NullKeyException() {
    }

    public NullKeyException(String message) {
        super(message);
    }

    public NullKeyException(String message, Throwable cause) {
        super(message, cause);
    }

    public NullKeyException(Throwable cause) {
        super(cause);
    }

    public NullKeyException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

}
