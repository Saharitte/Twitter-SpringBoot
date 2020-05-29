package co.jp.sahar.tweet.utils.exceptions;

public class KeywordNotFoundException extends RuntimeException {


    public KeywordNotFoundException() {
        super();
    }

    public KeywordNotFoundException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public KeywordNotFoundException(final String message) {
        super(message);
    }

    public KeywordNotFoundException(final Throwable cause) {
        super(cause);
    }

}
