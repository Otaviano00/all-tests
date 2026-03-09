package otav.br.infrastructure.servicebus;

import com.azure.messaging.servicebus.*;
import io.quarkus.runtime.Startup;
import jakarta.enterprise.context.ApplicationScoped;

import java.util.HashMap;
import java.util.Map;

@Startup
@ApplicationScoped
public class ServiceBusProducerManager {

    private Map<String, ServiceBusSenderClient> senderClients = new HashMap<>();

    public void sendMessage(String queueName, String connectionString, String message) {
        if (!senderClients.containsKey(queueName)) {
            ServiceBusSenderClient senderClient = new ServiceBusClientBuilder()
                    .connectionString(connectionString)
                    .sender()
                    .queueName(queueName)
                    .buildClient();
            senderClients.put(queueName, senderClient);
        }

        ServiceBusSenderClient senderClient = senderClients.get(queueName);
        senderClient.sendMessage(new ServiceBusMessage(message));
    }

}
