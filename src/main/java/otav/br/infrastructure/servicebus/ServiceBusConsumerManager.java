package otav.br.infrastructure.servicebus;

import com.azure.messaging.servicebus.*;
import io.quarkus.logging.Log;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import lombok.Getter;
import otav.br.infrastructure.servicebus.config.ServiceBusConfig;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

@Getter
@ApplicationScoped
public class ServiceBusConsumerManager {

    private record Key(String namespaceConnectionString, String queueName) {}

    private final Map<Key, ServiceBusResourceHolder<ServiceBusProcessorClient>> resources = new ConcurrentHashMap<>();

    // scheduler único e confiável
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "servicebus-consumer-restart");
        t.setDaemon(true);
        return t;
    });

    @PreDestroy
    public void shutdown() {
        Log.info("Shutting down Service Bus consumers...");
        resources.values().forEach(holder -> {
            if (holder.getResource() != null) holder.getResource().close();
        });
        resources.clear();

        scheduler.shutdownNow();
    }

    public void start(ServiceBusConfig.NamespaceConfig namespaceConfig,
                      ServiceBusConfig.QueueConfig queueConfig,
                      Consumer<ServiceBusReceivedMessageContext> processMessage,
                      Consumer<ServiceBusErrorContext> processError) {

        var key = new Key(namespaceConfig.connectionString(), queueConfig.name());

        resources.compute(key, (k, existing) -> {
            if (existing != null && existing.getResource() != null && existing.getResource().isRunning()) {
                return existing;
            }

            var builder = new ServiceBusClientBuilder()
                    .connectionString(namespaceConfig.connectionString())
                    .processor()
                    .queueName(queueConfig.name());

            if (processMessage != null) builder.processMessage(processMessage);
            builder.processError(processError == null ? this::defaultProcessError : processError);

            var processorClient = builder.buildProcessorClient();
            processorClient.start();

            Log.infof("Service Bus consumer started for queue=%s", queueConfig.name());
            return new ServiceBusResourceHolder<>(processorClient, namespaceConfig, queueConfig);
        });
    }

    public void close(ServiceBusConfig.NamespaceConfig namespaceConfig, ServiceBusConfig.QueueConfig queueConfig) {
        var key = new Key(namespaceConfig.connectionString(), queueConfig.name());
        var holder = resources.remove(key);
        if (holder != null && holder.getResource() != null) {
            holder.getResource().close();
        }
    }

    public void closeAndScheduleRestart(ServiceBusConfig.NamespaceConfig namespaceConfig,
                                        ServiceBusConfig.QueueConfig queueConfig,
                                        Consumer<ServiceBusReceivedMessageContext> processMessage,
                                        Consumer<ServiceBusErrorContext> processError,
                                        int delaySeconds) {

        var key = new Key(namespaceConfig.connectionString(), queueConfig.name());

        // um flag por recurso (se o seu holder já tem, ok — mas precisa existir no holder atual do map)
        var holder = resources.get(key);
        if (holder == null) {
            // se não existe ainda, não tenta reiniciar; alternativa: criar um placeholder com flag global por key
            return;
        }

        AtomicBoolean flag = holder.getRestartScheduled();
        if (flag == null || !flag.compareAndSet(false, true)) {
            return;
        }

        close(namespaceConfig, queueConfig);

        scheduler.schedule(() -> {
            try {
                Log.warnf("Restarting Service Bus consumer after %ds for queue=%s. now=%s thread=%s",
                        delaySeconds, queueConfig.name(), Instant.now(), Thread.currentThread().getName()
                );
                start(namespaceConfig, queueConfig, processMessage, processError);
            } finally {
                flag.set(false);
            }
        }, delaySeconds, TimeUnit.SECONDS);
    }

    private void defaultProcessError(ServiceBusErrorContext errorContext) {
        Log.errorf(errorContext.getException(), "Service Bus consumer error. Queue=%s", errorContext.getEntityPath());
    }
}