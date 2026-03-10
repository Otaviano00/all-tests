package otav.br.infrastructure.servicebus.resilience;

public record ServiceBusResilienceDecision(
        Disposition disposition,
        boolean restartConsumer,
        int restartDelaySeconds,
        String transferQueue,
        int retrayDelay,
        String deadLetterReason,
        String deadLetterDescription
) {
    public enum Disposition { COMPLETE, ABANDON, DEAD_LETTER, TRANSFER }
}