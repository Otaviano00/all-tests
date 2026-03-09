package otav.br.messaging.ibmmq;

import com.ibm.msg.client.jakarta.jms.JmsConstants;
import io.quarkus.logging.Log;
import io.quarkus.runtime.Startup;
import io.smallrye.common.annotation.RunOnVirtualThread;
import io.smallrye.reactive.messaging.annotations.Blocking;
import io.smallrye.reactive.messaging.jms.IncomingJmsMessage;
import io.smallrye.reactive.messaging.jms.IncomingJmsMessageMetadata;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;

import java.util.concurrent.CompletionStage;

@Startup
@ApplicationScoped
public class IBMMQConsumer {

    @Incoming("ibm-mq-in")
    @RunOnVirtualThread
    @Blocking("my-custom-pool")
    public CompletionStage<Void> consume(IncomingJmsMessage<String> message) {
        try {
            Log.infof("Received message from IBM MQ: " + message.getPayload());
        } catch (Exception e) {
            Log.errorf(e, "Error processing message: %s", e.getMessage());
        }
        return message.ack();
    }
}
