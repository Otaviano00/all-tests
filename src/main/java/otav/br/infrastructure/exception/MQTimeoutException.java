package otav.br.infrastructure.exception;

public class MQTimeoutException extends RuntimeException {
    public MQTimeoutException(String message) {
        super(message);
    }

    public MQTimeoutException(String message, Throwable cause) {
        super(message, cause);
    }
}
