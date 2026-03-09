package otav.br.resource.ibmmq;

import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.Response;
import otav.br.messaging.ibmmq.IBMMQConsumer;
import otav.br.messaging.ibmmq.IBMMQProducer;

@Path("ibm-mq/queue")
public class IBMMQResource {

    @Inject
    IBMMQProducer producer;

    @Inject
    IBMMQConsumer consumer;

    @POST
    public Response sendMessage(
            String message,
            @QueryParam("amount") @DefaultValue("1") int amount
    ) {
        for (int i = 0; i < amount; i++) {
            producer.produce(i + " - " + message);
        }
        return Response.ok("Message sent to IBM MQ: " + message).build();
    }

}
