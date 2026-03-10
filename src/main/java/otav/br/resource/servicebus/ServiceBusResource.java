package otav.br.resource.servicebus;

import jakarta.ws.rs.*;
import jakarta.ws.rs.core.Response;
import lombok.AllArgsConstructor;
import otav.br.messaging.ibmmq.ModalityEnum;
import otav.br.messaging.servicebus.OtavTestMessage;
import otav.br.messaging.servicebus.OtavTestProducer;

@Path("servicebus/queue")
@AllArgsConstructor
public class ServiceBusResource {

    private OtavTestProducer producer;

    @POST
    public Response sendMessage(
            String message,
            @QueryParam("amount") @DefaultValue("1") int amount
    ) {
        for (int i = 0; i < amount; i++) {
            var otavTestMessage = OtavTestMessage.builder()
                    .modality(i % 2 == 0 ? ModalityEnum.DEV1 : ModalityEnum.DEV2)
                    .content(i + " - " + message)
                    .build();

            producer.sendMessage(otavTestMessage);
        }
        return Response.ok("Message sent to IBM MQ: " + message).build();
    }
}
