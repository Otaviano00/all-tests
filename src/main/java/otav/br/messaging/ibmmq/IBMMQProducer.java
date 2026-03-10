package otav.br.messaging.ibmmq;

import com.ibm.mq.MQException;
import com.ibm.mq.constants.MQConstants;
import io.quarkus.logging.Log;
import io.quarkus.runtime.Startup;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.jms.Destination;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSException;
import jakarta.jms.JMSProducer;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import org.eclipse.microprofile.faulttolerance.CircuitBreaker;
import org.eclipse.microprofile.faulttolerance.Retry;
import otav.br.infrastructure.exception.MQPutException;
import otav.br.infrastructure.exception.MQTimeoutException;
import otav.br.infrastructure.ibmmq.IBMMQConnectionFactory;
import otav.br.infrastructure.ibmmq.config.IBMMQConfig;
import otav.br.messaging.servicebus.OtavTestMessage;

import java.time.temporal.ChronoUnit;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@Startup
@ApplicationScoped
public class IBMMQProducer {

    private volatile JMSContext jmsContext;
    private volatile JMSProducer jmsProducer;

    private IBMMQConfig.QueueManagerConfig queueManagerConfig;

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private final AtomicBoolean reconnectScheduled = new AtomicBoolean(false);

    private static final Set<Integer> UNCERTAIN_PUT_REASONS = Set.of(
            MQConstants.MQRC_CONNECTION_BROKEN,      // 2009
            MQConstants.MQRC_HCONN_ERROR,            // 2018
            MQConstants.MQRC_Q_MGR_NOT_AVAILABLE,    // 2059
            MQConstants.MQRC_Q_MGR_QUIESCING,        // 2161
            MQConstants.MQRC_UNEXPECTED_ERROR        // 2195
    );

    private static final Set<Integer> DEFINITIVE_NO_PUT_REASONS = Set.of(
            MQConstants.MQRC_Q_FULL,                 // 2050
            MQConstants.MQRC_PUT_INHIBITED,          // 2051
            MQConstants.MQRC_NOT_AUTHORIZED,         // 2035
            MQConstants.MQRC_UNKNOWN_OBJECT_NAME,    // 2085
            MQConstants.MQRC_OBJECT_DAMAGED          // 2101 (ou correlatos)
    );

    private static final Set<Integer> CONNECTION_ISSUE_REASONS = Set.of(
            MQConstants.MQRC_CONNECTION_BROKEN,      // 2009
            MQConstants.MQRC_HCONN_ERROR,            // 2018
            MQConstants.MQRC_RECONNECT_TIMED_OUT     // 2556
    );

    private IBMMQConnectionFactory connectionFactory;
    private IBMMQConfig ibmMQConfig;

    @Inject
    public IBMMQProducer(
                IBMMQConnectionFactory connectionFactory,
                IBMMQConfig ibmmqConfig
    ){
        this.connectionFactory = connectionFactory;
        this.ibmMQConfig = ibmmqConfig;
    }

    @PostConstruct
    public synchronized void init() {
        try {
            cleanup();

            queueManagerConfig = ibmMQConfig.queueManagers().get("QM8");

            this.jmsContext = connectionFactory.createConnectionFactory(queueManagerConfig)
                    .createContext(Session.AUTO_ACKNOWLEDGE);
            this.jmsProducer = jmsContext.createProducer().setTimeToLive(0);

            Log.info("IBM MQ JMS producer initialized");
        } catch (Exception e) {
            Log.errorf(e, "Error creating JMS context/producer: %s", e.getMessage());
            throw new RuntimeException("Failed to initialize IBM MQ producer", e);
        } finally {
            // libera novos agendamentos após qualquer tentativa de init
            reconnectScheduled.set(false);
        }
    }

    @Retry(
            retryOn = MQPutException.class,
            maxRetries = 3,
            delay = 2000,
            maxDuration = 10,
            durationUnit = ChronoUnit.SECONDS
    )
    @CircuitBreaker(requestVolumeThreshold = 10, failureRatio = 0.5)
    public void sendMessage(OtavTestMessage message) {
        try {
            String queueName = message.getModality().getQueueNameByModality(queueManagerConfig);
            Destination destination = jmsContext.createQueue(queueName);
            TextMessage textMessage = jmsContext.createTextMessage(message.getContent());
            jmsProducer.send(destination, textMessage);
            Log.infof("Sent message to IBM MQ: %s -> %s", message.getModality(), message.getContent());
        } catch (Exception e) {
            MQException mq = findMQException(e);

            if (mq == null) {
                Log.errorf(e, "Non-MQ exception sending message to IBM MQ: %s", e.getMessage());
                throw new MQPutException("Non-MQ exception while sending message to IBM MQ", e);
            }

            int compCode = mq.getCompCode();
            int reason = mq.getReason();

            Log.errorf(mq, "MQ error sending message (reason=%d, compCode=%d)", reason, compCode);

            if (isConnectionIssue(reason)) {
                scheduleReconnect();
            }

            if (isUncertainPut(reason)) {
                throw new MQTimeoutException(
                        "Uncertain outcome sending to IBM MQ (reason=" + reason + ", compCode=" + compCode + ")", mq
                );
            }

            if (isDefinitiveNoPut(reason)) {
                throw new MQPutException(
                        "Definitive failure sending to IBM MQ (reason=" + reason + ", compCode=" + compCode + ")", mq
                );
            }

            throw new MQPutException(
                    "Failed to send message to IBM MQ (reason=" + reason + ", compCode=" + compCode + ")", mq
            );
        }
    }

    private void scheduleReconnect() {
        if (!reconnectScheduled.compareAndSet(false, true)) {
            return; // já agendado
        }

        // Fecha recursos imediatamente para evitar reusar contexto inválido
        cleanup();

        scheduler.schedule(() -> {
            try {
                init();
            } catch (RuntimeException re) {
                // init já loga; aqui só garante que dá para reagendar em nova falha
                reconnectScheduled.set(false);
                throw re;
            }
        }, 30, TimeUnit.SECONDS);
    }

    @PreDestroy
    public synchronized void cleanup() {
        try {
            if (jmsContext != null) {
                jmsContext.close();
            }
        } catch (Exception e) {
            Log.debugf(e, "Error closing JMSContext: %s", e.getMessage());
        } finally {
            jmsContext = null;
            jmsProducer = null;
        }
    }

    private static boolean isUncertainPut(int reason) {
        return UNCERTAIN_PUT_REASONS.contains(reason);
    }

    private static boolean isDefinitiveNoPut(int reason) {
        return DEFINITIVE_NO_PUT_REASONS.contains(reason);
    }

    private static boolean isConnectionIssue(int reason) {
        return CONNECTION_ISSUE_REASONS.contains(reason);
    }

    private static MQException findMQException(Throwable t) {
        Throwable cur = t;
        while (cur != null) {
            if (cur instanceof MQException mq) {
                return mq;
            }
            if (cur instanceof JMSException jms) {
                Exception linked = jms.getLinkedException();
                if (linked instanceof MQException mq) {
                    return mq;
                }
            }
            cur = cur.getCause();
        }
        return null;
    }
}