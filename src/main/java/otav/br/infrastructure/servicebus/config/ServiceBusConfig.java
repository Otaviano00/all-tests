package otav.br.infrastructure.servicebus.config;

import io.smallrye.config.ConfigMapping;

import java.util.Map;

@ConfigMapping(prefix = "servicebus")
public interface ServiceBusConfig {

    Map<String, NamespaceConfig> namespaces();

    interface NamespaceConfig {
        Map<String, QueueConfig> queue();
        String connectionString();
    }

    interface QueueConfig {
        String name();
        ResilienceConfig resilience();
    }

    interface ResilienceConfig {
        String timeoutQueue();
        int timeoutRetryDelaySeconds();
        int restartDelaySeconds();
    }

}
