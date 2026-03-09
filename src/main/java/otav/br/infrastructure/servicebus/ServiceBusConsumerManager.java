package otav.br.infrastructure.servicebus;

import com.azure.messaging.servicebus.*;
import jakarta.enterprise.context.ApplicationScoped;
import lombok.Getter;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

@ApplicationScoped
@Getter
public class ServiceBusConsumer {

    private Map<String, ServiceBusProcessorClient> processorClients = new HashMap<>();
    
    public void start(String queueName, String connectionString, Consumer<ServiceBusReceivedMessageContext> processMessage, Consumer<ServiceBusErrorContext> processError) {
        // Create an instance of the processor through the ServiceBusClientBuilder
        var builder = new ServiceBusClientBuilder()
                .connectionString(connectionString)
                .processor()
                .queueName(queueName);

        if (processMessage != null) {
            builder.processMessage(processMessage);
        }

        if (processError != null) {
            builder.processError(processError);
        }

        var processorClient = builder.buildProcessorClient();

        processorClient.start();
        processorClients.put(queueName, processorClient);
    }

    public void stop(String queueName) {
        if (processorClients.containsKey(queueName)) {
            processorClients.get(queueName).close();
            processorClients.remove(queueName);
        }
    }

}
