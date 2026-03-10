package otav.br.infrastructure.servicebus;

import com.azure.messaging.servicebus.ServiceBusReceivedMessageContext;
import com.azure.messaging.servicebus.models.DeadLetterOptions;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import otav.br.infrastructure.servicebus.config.ServiceBusConfig;
import otav.br.infrastructure.servicebus.resilience.ServiceBusResilienceDecision;
import otav.br.infrastructure.servicebus.resilience.ServiceBusResiliencePolicy;

import java.util.function.Consumer;
import java.util.function.Function;

@ApplicationScoped
public class ServiceBusMessageDispatcher {

    @Inject
    ServiceBusResiliencePolicy policy;

    @Inject
    ServiceBusConsumerManager consumerManager;

    public <T> Consumer<ServiceBusReceivedMessageContext> dispatchingHandler(
            ServiceBusConfig.QueueConfig queueConfig,
            Function<ServiceBusReceivedMessageContext, T> deserializer,
            Consumer<T> handler
    ) {
        return ctx -> {
            String messageId = ctx.getMessage().getMessageId();

            Log.infof("[%s] Received from Service Bus: %s", queueConfig.name(), messageId);

            final T payload;
            try {
                payload = deserializer.apply(ctx);
            } catch (Exception e) {
                Log.errorf(e, "Deserialize failed. Dead-lettering messageId=%s", messageId);
                ctx.deadLetter(new DeadLetterOptions()
                        .setDeadLetterReason("DESERIALIZATION_ERROR")
                        .setDeadLetterErrorDescription(e.getMessage())
                );
                return;
            }

            try {
                handler.accept(payload);
                ctx.complete();
            } catch (Throwable t) {
                ServiceBusResilienceDecision d = policy.decide(t, queueConfig);

                switch (d.disposition()) {
                    case COMPLETE -> ctx.complete();
                    case ABANDON -> ctx.abandon();
                    case DEAD_LETTER -> {
                        if (d.deadLetterReason() != null || d.deadLetterDescription() != null) {
                            ctx.deadLetter(buildDeadLetterOptions(d));
                        } else {
                            ctx.deadLetter();
                        }
                    }
                    case TRANSFER -> {
                        if (d.transferQueue() == null) {
                            // Reenvia para a mesma fila com um delay
                            d.retrayDelay();
                        }

                        // Enviar para a fila de transferência configurada com o respectivo delay
                    }
                }

                if (d.restartConsumer()) {
                    consumerManager.stopAndScheduleRestart(queueConfig.name(), d.restartDelaySeconds());
                }
            }
        };
    }

    private DeadLetterOptions buildDeadLetterOptions(ServiceBusResilienceDecision decision) {
        return new DeadLetterOptions()
                .setDeadLetterReason(decision.deadLetterReason())
                .setDeadLetterErrorDescription(decision.deadLetterDescription());
    }
}