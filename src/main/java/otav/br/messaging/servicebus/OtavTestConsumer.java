package otav.br.messaging.servicebus;

import com.azure.messaging.servicebus.ServiceBusErrorContext;
import com.azure.messaging.servicebus.ServiceBusReceivedMessage;
import com.azure.messaging.servicebus.ServiceBusReceivedMessageContext;
import io.quarkus.logging.Log;
import io.quarkus.runtime.Startup;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.faulttolerance.exceptions.CircuitBreakerOpenException;
import otav.br.infrastructure.exception.MQPutException;
import otav.br.infrastructure.exception.MQTimeoutException;
import otav.br.infrastructure.servicebus.ServiceBusConfig;
import otav.br.infrastructure.servicebus.ServiceBusConsumer;
import otav.br.messaging.ibmmq.IBMMQProducer;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@Startup
@ApplicationScoped
public class OtavTestConsumer {

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private final AtomicBoolean restartScheduled = new AtomicBoolean(false);

    @Inject
    ServiceBusConsumer serviceBusConsumer;

    @Inject
    IBMMQProducer ibmMqProducer;

    @Inject
    ServiceBusConfig serviceBusConfig;

    private String queueName;
    private String connectionString;

    @PostConstruct
    public synchronized void init() {
        ServiceBusConfig.QueueConfig queueConfig = serviceBusConfig
                .namespaces()
                .get("otav.dev")
                .queue()
                .get("otav.test");

        this.queueName = queueConfig.name();
        this.connectionString = queueConfig.connectionString();

        startConsumerIfNeeded();
    }

    private synchronized void startConsumerIfNeeded() {
        if (serviceBusConsumer.getProcessorClients().containsKey(queueName)) {
            return;
        }

        Log.infof("Starting Service Bus consumer for queue=%s", queueName);
        serviceBusConsumer.start(queueName, connectionString, this::processMessage, this::processError);
    }

    public void processMessage(ServiceBusReceivedMessageContext context) {
        ServiceBusReceivedMessage message = context.getMessage();

        // Preferível: logar messageId e correlationId (e não o body inteiro se for sensível)
        Log.infof(
                "Received message from Service Bus: deliveryCount=%d messageId=%s",
                message.getDeliveryCount(),
                message.getMessageId()
        );

        try {
            ibmMqProducer.produce(message.getBody().toString());

            // Sucesso: confirma no Service Bus
            context.complete();
        } catch (CircuitBreakerOpenException e) {
            Log.warnf("Circuit breaker open. Abandoning messageId=%s and scheduling consumer restart. %s",
                    message.getMessageId(), e.getMessage());

            // Para reprocessar depois
            context.abandon();

            // Para de consumir e agenda restart com dedupe
            stopAndScheduleRestart(30);
        } catch (MQTimeoutException e) {
            Log.errorf(e,
                    "Uncertain MQ PUT outcome. Dead-lettering messageId=%s to avoid duplicates.",
                    message.getMessageId()
            );

            context.deadLetter();
        } catch (MQPutException e) {
            // CERTEZA: put não aconteceu => pode tentar de novo
            Log.errorf(e,
                    "Definitive MQ PUT failure. Abandoning messageId=%s for retry.",
                    message.getMessageId()
            );
            context.abandon();
        } catch (Exception e) {
            // Desconhecido: por segurança, não duplicar ou não perder?
            // Aqui eu assumo "não sei" e prefiro dead-letter para não gerar loop.
            Log.errorf(e,
                    "Unexpected error processing messageId=%s. Dead-lettering to avoid poison-message loop.",
                    message.getMessageId()
            );
            context.deadLetter();
        }
    }

    public void processError(ServiceBusErrorContext context) {
        Log.errorf(context.getException(), "Service Bus processor error: %s", context.getException().getMessage());
    }

    private void stopAndScheduleRestart(int delaySeconds) {
        if (!restartScheduled.compareAndSet(false, true)) {
            return;
        }

        try {
            serviceBusConsumer.stop(queueName);
        } catch (Exception e) {
            Log.debugf(e, "Error stopping Service Bus consumer: %s", e.getMessage());
        }

        scheduler.schedule(() -> {
            try {
                Log.infof("Restarting Service Bus consumer after %ds...", delaySeconds);
                startConsumerIfNeeded();
            } finally {
                restartScheduled.set(false);
            }
        }, delaySeconds, TimeUnit.SECONDS);
    }

    @PreDestroy
    public void cleanup() {
        try {
            serviceBusConsumer.stop(queueName);
        } catch (Exception e) {
            Log.debugf(e, "Error stopping Service Bus consumer on shutdown: %s", e.getMessage());
        }
        scheduler.shutdown();
    }
}