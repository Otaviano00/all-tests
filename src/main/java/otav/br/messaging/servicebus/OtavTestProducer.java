package otav.br.messaging.servicebus;

import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import lombok.AllArgsConstructor;
import otav.br.infrastructure.servicebus.ServiceBusConfig;
import otav.br.infrastructure.servicebus.ServiceBusProducer;

@ApplicationScoped
@AllArgsConstructor
public class OtavTestProducer {

    private ServiceBusProducer serviceBusProducer;
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

        serviceBusProducer.sendMessage(queueName, connectionString, message);
        Log.infof("Sent message to Service Bus queue '%s' in namespace 'otav.dev': %s", queueName, message);
    }

}
