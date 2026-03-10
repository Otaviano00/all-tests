package otav.br.resource.ibmmq;

import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.Response;
import otav.br.messaging.ibmmq.IBMMQConsumer;
import otav.br.messaging.ibmmq.IBMMQProducer;
import otav.br.messaging.ibmmq.ModalityEnum;
import otav.br.messaging.servicebus.OtavTestMessage;

@Path("ibm-mq/queue")
public class IBMMQResource {

    @Inject
    IBMMQProducer producer;

    @POST
    public Response sendMessage(
            String message,
            @QueryParam("amount") @DefaultValue("1") int amount
    ) {
        for (int i = 0; i < amount; i++) {
            var otavTestMessage = OtavTestMessage.builder()
                    .modality(ModalityEnum.DEV1)
                    .content(i + " - " + message)
                    .build();
            producer.sendMessage(otavTestMessage);
        }
        return Response.ok("Message sent to IBM MQ: " + message).build();
    }

}
