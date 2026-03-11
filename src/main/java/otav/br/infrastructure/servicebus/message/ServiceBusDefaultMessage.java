package otav.br.infrastructure.servicebus.message;

import lombok.Data;

import java.util.HashMap;
import java.util.Map;

@Data
public class ServiceBusDefaultMessage {
    private Map<String, String> applicationProperties = new HashMap<>();
}
