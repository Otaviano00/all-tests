package otav.br.messaging.servicebus;

import com.azure.messaging.servicebus.ServiceBusReceivedMessageContext;
import com.azure.messaging.servicebus.models.DeadLetterOptions;
import io.quarkus.logging.Log;
import io.quarkus.runtime.Startup;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.faulttolerance.exceptions.CircuitBreakerOpenException;
import otav.br.infrastructure.exception.MQPutException;
import otav.br.infrastructure.exception.MQTimeoutException;
import otav.br.infrastructure.servicebus.ServiceBusConsumerManager;
import otav.br.infrastructure.servicebus.ServiceBusMessageDispatcher;
import otav.br.infrastructure.servicebus.config.ServiceBusConfig;
import otav.br.service.DevService;

@Startup
@ApplicationScoped
public class OtavTestConsumer {

    @Inject ServiceBusConsumerManager serviceBusConsumerManager;
    @Inject ServiceBusMessageDispatcher dispatcher;
    @Inject ServiceBusConfig serviceBusConfig;
    @Inject DevService devService;

    private ServiceBusConfig.QueueConfig queueConfig;
    private ServiceBusConfig.NamespaceConfig namespaceConfig;

    @PostConstruct
    public void init() {
        namespaceConfig = serviceBusConfig.namespaces().get("otav.dev");
        queueConfig = namespaceConfig.queue().get("otav.test");

        serviceBusConsumerManager.start(
                namespaceConfig,
                queueConfig,
                this::processMessage,
                null
        );
    }

    public void processMessage(ServiceBusReceivedMessageContext context) {
        var processMessage = dispatcher.dispatchingHandler(
                queueConfig,
                ctx -> ctx.getMessage().getBody().toObject(OtavTestMessage.class),
                devService::mockProcess
        );

        var message = context.getMessage();

        try {
            processMessage.accept(context);
        } catch (CircuitBreakerOpenException e) {
            Log.warnf("[%s] Circuit breaker open. Abandoning messageId=%s and scheduling consumer restart. %s",
                    queueConfig.name(), message.getMessageId(), e.getMessage()
            );

            context.abandon();

            serviceBusConsumerManager.closeAndScheduleRestart(
                    namespaceConfig,
                    queueConfig,
                    this::processMessage,
                    null,
                    queueConfig.resilience().restartDelaySeconds()
            );
        } catch (MQTimeoutException e) {
            Log.errorf(e, "[%s] Uncertain MQ PUT outcome. Dead-lettering messageId=%s to avoid duplicates.",
                    queueConfig.name(), message.getMessageId()
            );

            context.deadLetter();
        } catch (MQPutException e) {
            Log.errorf(e, "[%s] Definitive MQ PUT failure. Abandoning messageId=%s for retry.",
                    queueConfig.name(), message.getMessageId()
            );
            context.abandon();
        } catch (Exception e) {
            Log.errorf(e,
                    "[%s] Unexpected error processing messageId=%s. Dead-lettering to avoid poison-message loop.",
                    queueConfig.name(), message.getMessageId()
            );
            context.deadLetter();
        }

    }
}