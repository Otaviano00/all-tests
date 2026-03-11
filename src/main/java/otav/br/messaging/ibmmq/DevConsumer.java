package otav.br.messaging.ibmmq;

import io.quarkus.logging.Log;
import io.quarkus.runtime.Startup;
import io.smallrye.common.annotation.RunOnVirtualThread;
import io.smallrye.reactive.messaging.jms.IncomingJmsMessage;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.reactive.messaging.Incoming;

import java.util.concurrent.CompletionStage;

@Startup
@ApplicationScoped
public class DevConsumer {

    @Incoming("ibm-mq-in-1")
    @Incoming("ibm-mq-in-2")
    @RunOnVirtualThread
    public CompletionStage<Void> consume1(IncomingJmsMessage<String> message) {
        try {
            Log.infof("Received message from IBM MQ: %s", message.getPayload());
        } catch (Exception e) {
            Log.errorf(e, "Error processing message: %s", message.getPayload());
        }
        return message.ack();
    }
}
