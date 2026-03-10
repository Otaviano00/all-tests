package otav.br.infrastructure.servicebus;

import com.azure.messaging.servicebus.*;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.runtime.Startup;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.HashMap;
import java.util.Map;

@Startup
@ApplicationScoped
public class ServiceBusProducerManager {

    @Inject
    private ObjectMapper objectMapper;

    private Map<String, ServiceBusSenderClient> senderClients = new HashMap<>();

    public void sendMessage(String queueName, String connectionString, ServiceBusDefaultMessage message) {
        if (!senderClients.containsKey(queueName)) {
            ServiceBusSenderClient senderClient = new ServiceBusClientBuilder()
                    .connectionString(connectionString)
                    .sender()
                    .queueName(queueName)
                    .buildClient();
            senderClients.put(queueName, senderClient);
        }

        ServiceBusSenderClient senderClient = senderClients.get(queueName);
        try {
            var json = objectMapper.writeValueAsString(message);
            senderClient.sendMessage(new ServiceBusMessage(json));
        } catch (Exception e ) {
            throw new RuntimeException("Failed to send message to Service Bus queue '" + queueName + "': " + e.getMessage(), e);
        }
    }

}
