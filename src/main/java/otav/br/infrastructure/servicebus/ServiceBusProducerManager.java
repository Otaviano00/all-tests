package otav.br.infrastructure.servicebus;

import com.azure.messaging.servicebus.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.runtime.Startup;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import otav.br.infrastructure.servicebus.config.ServiceBusConfig;
import otav.br.infrastructure.servicebus.message.ServiceBusDefaultMessage;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

@Startup
@ApplicationScoped
public class ServiceBusProducerManager {

    @Inject ObjectMapper objectMapper;

    private List<ServiceBusResourceHolder<ServiceBusSenderClient>> resources = new ArrayList<>();

    @PreDestroy
    public void shutdown() {
        resources.forEach(holder -> {
            if (holder.getResource() != null) {
                holder.getResource().close();
            }
        });
        resources.clear();
    }

    public void sendMessage(ServiceBusConfig.NamespaceConfig namespaceConfig,
                            ServiceBusConfig.QueueConfig queueConfig,
                            Function<ServiceBusDefaultMessage, String> serializer,
                            ServiceBusDefaultMessage message
    ) {
        var holder = findResource(namespaceConfig, queueConfig);

        if (holder == null) {
            ServiceBusSenderClient senderClient = new ServiceBusClientBuilder()
                    .connectionString(namespaceConfig.connectionString())
                    .sender()
                    .queueName(queueConfig.name())
                    .buildClient();
            holder = new ServiceBusResourceHolder<>(senderClient, namespaceConfig, queueConfig);
            resources.add(holder);
        }

        try {
            var json = serializer == null ?
                    objectMapper.writeValueAsString(message) :
                    serializer.apply(message);

            var serviceBusMessage = new ServiceBusMessage(json);
            serviceBusMessage.getApplicationProperties().putAll(message.getApplicationProperties());

            holder.getResource().sendMessage(serviceBusMessage);
        } catch (Exception e ) {
            throw new RuntimeException("Failed to send message to Service Bus queue '" + queueConfig.name() + "': " + e.getMessage(), e);
        }
    }

    private ServiceBusResourceHolder<ServiceBusSenderClient> findResource(ServiceBusConfig.NamespaceConfig namespaceConfig, ServiceBusConfig.QueueConfig queueConfig) {
        return resources.stream()
                .filter(r -> r.equals(namespaceConfig, queueConfig))
                .findFirst()
                .orElse(null);
    }

}
