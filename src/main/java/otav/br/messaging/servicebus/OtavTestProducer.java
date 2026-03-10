package otav.br.messaging.servicebus;

import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import lombok.AllArgsConstructor;
import otav.br.infrastructure.servicebus.config.ServiceBusConfig;
import otav.br.infrastructure.servicebus.ServiceBusProducerManager;

@ApplicationScoped
@AllArgsConstructor
public class OtavTestProducer {

    private ServiceBusProducerManager serviceBusProducerManager;
    private ServiceBusConfig serviceBusConfig;

    public void sendMessage(OtavTestMessage message) {
        ServiceBusConfig.NamespaceConfig namespaceConfig = serviceBusConfig
                .namespaces()
                .get("otav.dev");

        ServiceBusConfig.QueueConfig queueConfig = namespaceConfig
                .queue()
                .get("otav.test");

        String queueName = queueConfig.name();
        String connectionString = namespaceConfig.connectionString();

        serviceBusProducerManager.sendMessage(queueName, connectionString, message);
        Log.infof("[%s] Sent message to Service Bus: %s", queueName, message.toString());
    }

}
