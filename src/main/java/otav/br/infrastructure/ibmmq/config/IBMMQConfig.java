package otav.br.infrastructure.ibmmq.config;

import io.smallrye.config.ConfigMapping;

import java.util.Map;

@ConfigMapping(prefix = "ibmmq")
public interface IBMMQConfig {

    Map<String, QueueManagerConfig> queueManagers();

    interface QueueManagerConfig {
        String hostName();
        int port();
        String queueManager();
        String channel();
        String appName();
        String userId();
        String password();
        int transportType();

        Map<String, QueueConfig> queues();
    }

    interface QueueConfig {
        String name();
        String modality();
    }
}

