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

    public void sendMessage(String message) {
        // Access the configuration: namespace -> queue
        ServiceBusConfig.QueueConfig queueConfig = serviceBusConfig
                .namespaces()
                .get("otav.dev")
                .queue()
                .get("otav.test");

        String queueName = queueConfig.name();
        String connectionString = queueConfig.connectionString();

        serviceBusProducerManager.sendMessage(queueName, connectionString, message);
        Log.infof("Sent message to Service Bus queue '%s' in namespace 'otav.dev': %s", queueName, message);
    }

}
