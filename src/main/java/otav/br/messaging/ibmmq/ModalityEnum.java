package otav.br.messaging.ibmmq;

import otav.br.infrastructure.ibmmq.config.IBMMQConfig;

public enum ModalityEnum {
    DEV1,
    DEV2;

    public static ModalityEnum fromString(String modality) {
        for (ModalityEnum m : ModalityEnum.values()) {
            if (m.name().equals(modality)) {
                return m;
            }
        }
        throw new IllegalArgumentException("Unknown modality: " + modality);
    }

    public String getQueueNameByModality(IBMMQConfig.QueueManagerConfig queueManagerConfig) {
        for (IBMMQConfig.QueueConfig queueConfig : queueManagerConfig.queues().values()) {
            if (queueConfig.modality().equals(this.name())) {
                return queueConfig.name();
            }
        }
        throw new IllegalArgumentException("No queue found for modality: " + this.name());
    }
}
