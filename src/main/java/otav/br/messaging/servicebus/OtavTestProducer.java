package otav.br.messaging.servicebus;

import io.quarkus.logging.Log;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import otav.br.infrastructure.servicebus.config.ServiceBusConfig;
import otav.br.infrastructure.servicebus.ServiceBusProducerManager;

@ApplicationScoped
public class OtavTestProducer {

    @Inject ServiceBusProducerManager serviceBusProducerManager;
    @Inject ServiceBusConfig serviceBusConfig;

    private ServiceBusConfig.NamespaceConfig namespaceConfig;
    private ServiceBusConfig.QueueConfig queueConfig;

    @PostConstruct
    public void init() {
        namespaceConfig = serviceBusConfig
                .namespaces()
                .get("otav.dev");

        queueConfig = namespaceConfig
                .queue()
                .get("otav.test");
    }

    public void sendMessage(OtavTestMessage message) {
        serviceBusProducerManager.sendMessage(namespaceConfig, queueConfig, null, message);
        Log.infof("[%s] Sent message to Service Bus: %s", queueConfig.name(), message.toString());
    }

}
