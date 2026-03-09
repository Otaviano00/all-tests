package otav.br.infrastructure.exception;

public class MQPutException extends RuntimeException {
    public MQPutException(String message) {
        super(message);
    }

    public MQPutException(String message, Throwable cause) {
        super(message, cause);
    }
}
