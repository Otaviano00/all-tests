package otav.br.messaging.servicebus;

import com.azure.messaging.servicebus.ServiceBusErrorContext;
import io.quarkus.logging.Log;
import io.quarkus.runtime.Startup;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import otav.br.infrastructure.servicebus.ServiceBusConsumerManager;
import otav.br.infrastructure.servicebus.ServiceBusMessageDispatcher;
import otav.br.infrastructure.servicebus.config.ServiceBusConfig;
import otav.br.messaging.ibmmq.IBMMQProducer;

@Startup
@ApplicationScoped
public class OtavTestConsumer {

    @Inject ServiceBusConsumerManager serviceBusConsumerManager;
    @Inject ServiceBusMessageDispatcher dispatcher;
    @Inject ServiceBusConfig serviceBusConfig;
    @Inject IBMMQProducer ibmMqProducer;

    @PostConstruct
    public void init() {
        var namespaceConfig = serviceBusConfig.namespaces().get("otav.dev");
        var queueConfig = namespaceConfig.queue().get("otav.test");

        serviceBusConsumerManager.start(
                namespaceConfig,
                queueConfig,
                dispatcher.dispatchingHandler(
                        queueConfig,
                        ctx -> ctx.getMessage().getBody().toObject(OtavTestMessage.class),
                        ibmMqProducer::sendMessage
                ),
                this::processError
        );

        Log.infof("aaaa");
    }

    public void processError(ServiceBusErrorContext context) {
        Log.errorf(context.getException(), "Service Bus processor error: %s", context.getException().getMessage());
    }

    @PreDestroy
    public void cleanup() {
        var namespaceConfig = serviceBusConfig.namespaces().get("otav.dev");
        var queueConfig = namespaceConfig.queue().get("otav.test");
        serviceBusConsumerManager.stop(queueConfig.name());
    }
}