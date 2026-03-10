package otav.br.infrastructure.servicebus.resilience;

import otav.br.infrastructure.servicebus.config.ServiceBusConfig;

public interface ServiceBusResiliencePolicy {
    ServiceBusResilienceDecision decide(Throwable error, ServiceBusConfig.QueueConfig queueConfig);
}