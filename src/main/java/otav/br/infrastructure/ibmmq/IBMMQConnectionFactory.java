package otav.br.messaging.ibmmq;

import com.ibm.mq.jakarta.jms.MQQueueConnectionFactory;
import com.ibm.msg.client.jakarta.jms.JmsConstants;
import com.ibm.msg.client.jakarta.wmq.WMQConstants;
import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;

@ApplicationScoped
public class IBMMQConnectionFactory {

    @Identifier("ibm-mq-factory")
    @Produces
    public MQQueueConnectionFactory createConnectionFactory() throws Exception {
        MQQueueConnectionFactory factory = new MQQueueConnectionFactory();
        factory.setHostName("localhost");
        factory.setPort(1414);
        factory.setQueueManager("QM8");
        factory.setChannel("DEV.APP.SVRCONN");
        factory.setTransportType(1); // client mode

        factory.setAppName("test-app");
        factory.setStringProperty(JmsConstants.USERID, "app");
        factory.setStringProperty(JmsConstants.PASSWORD, "passw0rd");

        // Client reconnect (IBM MQ):
        factory.setIntProperty(WMQConstants.WMQ_CLIENT_RECONNECT_OPTIONS, WMQConstants.WMQ_CLIENT_RECONNECT);
        factory.setIntProperty(WMQConstants.WMQ_CLIENT_RECONNECT_TIMEOUT, 300);

        return factory;
    }
}