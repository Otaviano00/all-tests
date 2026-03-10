package otav.br.infrastructure.servicebus;

import com.azure.messaging.servicebus.*;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import lombok.Getter;
import otav.br.infrastructure.servicebus.config.ServiceBusConfig;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

@Getter
@ApplicationScoped
public class ServiceBusConsumerManager {

    private final Map<String, ServiceBusResourceHolder> resources = new ConcurrentHashMap<>();

    public void start(ServiceBusConfig.NamespaceConfig namespaceConfig,
                      ServiceBusConfig.QueueConfig queueConfig,
                      Consumer<ServiceBusReceivedMessageContext> processMessage,
                      Consumer<ServiceBusErrorContext> processError
    ) {
        String queueName = queueConfig.name();

        // Evita double-start
        ServiceBusResourceHolder existing = resources.get(queueName);
        if (existing != null && existing.processorClient != null && existing.processorClient.isRunning()) {
            return;
        }

        var builder = new ServiceBusClientBuilder()
                .connectionString(namespaceConfig.connectionString())
                .processor()
                .queueName(queueName);

        if (processMessage != null) builder.processMessage(processMessage);
        if (processError != null) builder.processError(processError);

        var processorClient = builder.buildProcessorClient();
        processorClient.start();

        resources.put(queueName, new ServiceBusResourceHolder(processorClient, namespaceConfig, queueConfig));

        Log.infof("Service Bus consumer started for queue=%s", queueName);
    }

    public void stop(String queueName) {
        ServiceBusResourceHolder holder = resources.remove(queueName);
        if (holder != null && holder.processorClient != null) {
            holder.stop();
        }
    }

    public void stopAndScheduleRestart(String queueName, int delaySeconds) {
        ServiceBusResourceHolder holder = resources.get(queueName);

        if (holder == null) return;

        if (!holder.restartScheduled.compareAndSet(false, true)) {
            return;
        }

        stop(queueName);

        holder.scheduler.schedule(() -> {
            try {
                Log.infof("Restarting Service Bus consumer after %ds for queue=%s...", delaySeconds, queueName);
                start(holder.namespaceConfig, holder.queueConfig, null, null);
            } finally {
                holder.restartScheduled.set(false);
            }
        }, delaySeconds, TimeUnit.SECONDS);
    }

    public class ServiceBusResourceHolder {
        private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        private final AtomicBoolean restartScheduled = new AtomicBoolean(false);

        private volatile ServiceBusProcessorClient processorClient;
        private final ServiceBusConfig.QueueConfig queueConfig;
        private final ServiceBusConfig.NamespaceConfig namespaceConfig;

        public ServiceBusResourceHolder(ServiceBusProcessorClient processorClient,
                                        ServiceBusConfig.NamespaceConfig namespaceConfig,
                                        ServiceBusConfig.QueueConfig queueConfig) {
            this.namespaceConfig = namespaceConfig;
            this.processorClient = processorClient;
            this.queueConfig = queueConfig;
        }

        private void stop() {
            try {
                if (processorClient != null) {
                    processorClient.stop();
                    processorClient.close();
                }
            } catch (Exception e) {
                Log.debugf(e, "Error stopping Service Bus consumer: %s", e.getMessage());
            } finally {
                processorClient = null;
            }
        }
    }
}