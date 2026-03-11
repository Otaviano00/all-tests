package otav.br.messaging.ibmmq;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.faulttolerance.CircuitBreaker;
import org.eclipse.microprofile.faulttolerance.Retry;
import otav.br.infrastructure.exception.MQPutException;
import otav.br.infrastructure.ibmmq.IBMMQProducer;

import java.time.temporal.ChronoUnit;

@ApplicationScoped
public class DevProducer {

    @Inject IBMMQProducer ibmmqProducer;

    @Retry(
            retryOn = MQPutException.class,
            maxRetries = 3,
            delay = 2000,
            maxDuration = 10,
            durationUnit = ChronoUnit.SECONDS
    )
    @CircuitBreaker(
        requestVolumeThreshold=10,
        failureRatio=0.5
    )
    public void sendMessage(String queueName, String message) {
        ibmmqProducer.sendMessage(queueName, message);
    }

}
