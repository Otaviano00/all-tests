package otav.br.infrastructure.servicebus;

import io.smallrye.config.ConfigMapping;

import java.util.Map;

@ConfigMapping(prefix = "servicebus")
public interface ServiceBusConfig {

    Map<String, NamespaceConfig> namespaces();

    interface NamespaceConfig {
        Map<String, QueueConfig> queue();
    }

    interface QueueConfig {
        String name();
        String connectionString();
    }

}
