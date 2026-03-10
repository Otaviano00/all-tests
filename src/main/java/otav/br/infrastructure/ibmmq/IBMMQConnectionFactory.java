package otav.br.infrastructure.ibmmq;

import com.ibm.mq.jakarta.jms.MQQueueConnectionFactory;
import com.ibm.msg.client.jakarta.jms.JmsConstants;
import com.ibm.msg.client.jakarta.wmq.WMQConstants;
import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import otav.br.infrastructure.ibmmq.config.IBMMQConfig;

@ApplicationScoped
public class IBMMQConnectionFactory {

    @Inject
    IBMMQConfig ibmMQConfig;

    @Produces
    @Identifier("ibm-mq-factory")
    public MQQueueConnectionFactory createConnectionFactory() throws Exception {
        IBMMQConfig.QueueManagerConfig qmConfig = ibmMQConfig.queueManagers().get("QM8");
        return createConnectionFactory(qmConfig);
    }

    public MQQueueConnectionFactory createConnectionFactory(IBMMQConfig.QueueManagerConfig qmConfig) throws Exception {
        MQQueueConnectionFactory factory = new MQQueueConnectionFactory();
        factory.setHostName(qmConfig.hostName());
        factory.setPort(qmConfig.port());
        factory.setQueueManager(qmConfig.queueManager());
        factory.setChannel(qmConfig.channel());
        factory.setTransportType(qmConfig.transportType());

        factory.setAppName(qmConfig.appName());
        factory.setStringProperty(JmsConstants.USERID, qmConfig.userId());
        factory.setStringProperty(JmsConstants.PASSWORD, qmConfig.password());

        // Client reconnect (IBM MQ):
        factory.setIntProperty(WMQConstants.WMQ_CLIENT_RECONNECT_OPTIONS, WMQConstants.WMQ_CLIENT_RECONNECT);
        factory.setIntProperty(WMQConstants.WMQ_CLIENT_RECONNECT_TIMEOUT, 300);

        return factory;
    }
}

