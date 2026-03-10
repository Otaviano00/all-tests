package otav.br.infrastructure.servicebus.resilience;

import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.faulttolerance.exceptions.CircuitBreakerOpenException;
import otav.br.infrastructure.exception.MQPutException;
import otav.br.infrastructure.exception.MQTimeoutException;
import otav.br.infrastructure.servicebus.config.ServiceBusConfig;

@ApplicationScoped
public class DefaultServiceBusResiliencePolicy implements ServiceBusResiliencePolicy {

    @Override
    public ServiceBusResilienceDecision decide(Throwable error, ServiceBusConfig.QueueConfig queueConfig) {
        if (error instanceof CircuitBreakerOpenException) {
            return new ServiceBusResilienceDecision(
                    ServiceBusResilienceDecision.Disposition.ABANDON,
                    true,
                    queueConfig.resilience().restartDelaySeconds(),
                    null,
                    0,
                    null,
                    null
            );
        }

        if (error instanceof MQTimeoutException) {
            // Sem idempotência no destino => DLQ sempre
            return new ServiceBusResilienceDecision(
                    ServiceBusResilienceDecision.Disposition.TRANSFER,
                    false,
                    0,
                    queueConfig.resilience().timeoutQueue(),
                    queueConfig.resilience().timeoutRetryDelaySeconds(),
                    "MQ_UNCERTAIN_PUT",
                    error.getMessage()
            );
        }

        if (error instanceof MQPutException) {
            return new ServiceBusResilienceDecision(
                    ServiceBusResilienceDecision.Disposition.TRANSFER,
                    false,
                    0,
                    null,
                    60,
                    null,
                    null
            );
        }

        return new ServiceBusResilienceDecision(
                ServiceBusResilienceDecision.Disposition.DEAD_LETTER,
                false,
                0,
                null,
                0,
                "UNEXPECTED_ERROR",
                error.getMessage()
        );
    }
}