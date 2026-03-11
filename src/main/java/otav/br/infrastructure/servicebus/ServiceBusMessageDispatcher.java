package otav.br.infrastructure.servicebus;

import com.azure.messaging.servicebus.ServiceBusReceivedMessageContext;
import com.azure.messaging.servicebus.models.DeadLetterOptions;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import otav.br.infrastructure.servicebus.config.ServiceBusConfig;

import java.util.function.Consumer;
import java.util.function.Function;

@ApplicationScoped
public class ServiceBusMessageDispatcher {

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
                Log.errorf(e, "[%s] Deserialize failed. Dead-lettering messageId=%s",
                        queueConfig.name(), messageId);
                ctx.deadLetter(new DeadLetterOptions()
                        .setDeadLetterReason("DESERIALIZATION_ERROR")
                        .setDeadLetterErrorDescription(e.getMessage())
                );
                return;
            }

            handler.accept(payload);
            ctx.complete();
        };
    }
}