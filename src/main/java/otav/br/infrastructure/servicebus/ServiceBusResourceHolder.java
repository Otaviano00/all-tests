package otav.br.infrastructure.servicebus;

import lombok.Getter;
import lombok.Setter;
import otav.br.infrastructure.servicebus.config.ServiceBusConfig;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

@Getter
@Setter
public class ServiceBusResourceHolder<R> {
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private final AtomicBoolean restartScheduled = new AtomicBoolean(false);

    private volatile R resource;
    private final ServiceBusConfig.QueueConfig queueConfig;
    private final ServiceBusConfig.NamespaceConfig namespaceConfig;

    public ServiceBusResourceHolder(R resource,
                                    ServiceBusConfig.NamespaceConfig namespaceConfig,
                                    ServiceBusConfig.QueueConfig queueConfig) {
        this.namespaceConfig = namespaceConfig;
        this.resource = resource;
        this.queueConfig = queueConfig;
    }

    public boolean equals(ServiceBusConfig.NamespaceConfig namespaceConfig, ServiceBusConfig.QueueConfig queueConfig) {
        return this.namespaceConfig.equals(namespaceConfig) && this.queueConfig.equals(queueConfig);
    }
}
