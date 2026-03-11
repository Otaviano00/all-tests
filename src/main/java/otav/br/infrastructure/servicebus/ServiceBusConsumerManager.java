package otav.br.infrastructure.servicebus;

import com.azure.messaging.servicebus.*;
import io.quarkus.logging.Log;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import lombok.Getter;
import otav.br.infrastructure.servicebus.config.ServiceBusConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

@Getter
@ApplicationScoped
public class ServiceBusConsumerManager {

    private final List<ServiceBusResourceHolder<ServiceBusProcessorClient>> resources = new ArrayList<>();

    @PreDestroy
    public void shutdown() {
        Log.info("Shutting down Service Bus consumers...");
        resources.forEach(holder -> {
            if (holder.getResource() != null) {
                holder.getResource().close();
            }
        });
        resources.clear();
    }

    public void start(ServiceBusConfig.NamespaceConfig namespaceConfig,
                      ServiceBusConfig.QueueConfig queueConfig,
                      Consumer<ServiceBusReceivedMessageContext> processMessage,
                      Consumer<ServiceBusErrorContext> processError
    ) {
        var existing = findResource(namespaceConfig, queueConfig);

        if (existing != null && existing.getResource() != null && existing.getResource().isRunning()) {
            return;
        }

        var builder = new ServiceBusClientBuilder()
                .connectionString(namespaceConfig.connectionString())
                .processor()
                .queueName(queueConfig.name());

        if (processMessage != null) builder.processMessage(processMessage);

        builder.processError(processError == null ? this::defaultProcessError : processError);

        var processorClient = builder.buildProcessorClient();
        processorClient.start();

        resources.add(new ServiceBusResourceHolder<>(processorClient, namespaceConfig, queueConfig));

        Log.infof("Service Bus consumer started for queue=%s", queueConfig.name());
    }

    public void close(ServiceBusConfig.NamespaceConfig namespaceConfig, ServiceBusConfig.QueueConfig queueConfig) {
        var holder = findResource(namespaceConfig, queueConfig);

        if (holder != null && holder.getResource() != null) {
            resources.removeIf(r ->
                    r.getNamespaceConfig().equals(namespaceConfig) && r.getQueueConfig().equals(queueConfig)
            );
            holder.getResource().close();
        }
    }

    public void closeAndScheduleRestart(
            ServiceBusConfig.NamespaceConfig namespaceConfig,
            ServiceBusConfig.QueueConfig queueConfig,
            Consumer<ServiceBusReceivedMessageContext> processMessage,
            Consumer<ServiceBusErrorContext> processError,
            int delaySeconds
    ) {
        var holder = findResource(namespaceConfig, queueConfig);

        if (holder == null || !holder.getRestartScheduled().compareAndSet(false, true)) {
            return;
        }

        close(namespaceConfig, queueConfig);

        holder.getScheduler().schedule(() -> {
            try {
                Log.infof("Restarting Service Bus consumer after %ds for queue=%s...", delaySeconds, queueConfig.name());
                start(holder.getNamespaceConfig(), holder.getQueueConfig(), processMessage, processError);
            } finally {
                holder.getRestartScheduled().set(false);
            }
        }, delaySeconds, TimeUnit.SECONDS);
    }

    private ServiceBusResourceHolder<ServiceBusProcessorClient> findResource(ServiceBusConfig.NamespaceConfig namespaceConfig, ServiceBusConfig.QueueConfig queueConfig) {
        return resources.stream()
                .filter(r -> r.equals(namespaceConfig, queueConfig))
                .findFirst()
                .orElse(null);
    }

    private void defaultProcessError(ServiceBusErrorContext errorContext) {
        Log.errorf(errorContext.getException(), "Service Bus consumer error. Queue=%s", errorContext.getEntityPath());
    }
}