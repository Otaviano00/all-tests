package otav.br.messaging.servicebus;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import otav.br.infrastructure.servicebus.ServiceBusDefaultMessage;
import otav.br.messaging.ibmmq.ModalityEnum;

@Getter
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class OtavTestMessage extends ServiceBusDefaultMessage {
    private ModalityEnum modality;
    private String content;
}
