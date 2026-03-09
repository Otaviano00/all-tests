package otav.br.resource.servicebus;

import jakarta.ws.rs.*;
import jakarta.ws.rs.core.Response;
import lombok.AllArgsConstructor;
import otav.br.messaging.servicebus.OtavTestConsumer;
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
            producer.sendMessage(i + " - " + message);
        }
        return Response.ok("Message sent to IBM MQ: " + message).build();
    }
}
