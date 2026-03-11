package otav.br.service;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import otav.br.messaging.ibmmq.DevProducer;
import otav.br.messaging.ibmmq.ModalityEnum;
import otav.br.messaging.servicebus.OtavTestMessage;

@ApplicationScoped
public class DevService {

    @Inject DevProducer devProducer;

    public void mockProcess(OtavTestMessage message) {
        var queueName = message.getModality() == ModalityEnum.DEV1 ? "DEV.QUEUE.1" : "DEV.QUEUE.2";

        // Simulate some processing logic here
        System.out.println("Processing message: " + message.getContent() + " for queue: " + queueName);
        try {
            Thread.sleep(500); // Simulate processing time
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        devProducer.sendMessage(queueName, message.getContent());
    }

}
