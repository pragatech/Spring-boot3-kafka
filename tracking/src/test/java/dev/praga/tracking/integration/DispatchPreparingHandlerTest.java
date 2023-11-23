package dev.praga.tracking.integration;

import dev.praga.tracking.TrackingConfiguration;
import dev.praga.tracking.handler.DispatchHandler;
import dev.praga.tracking.message.DispatchCompleted;
import dev.praga.tracking.message.DispatchPrepared;
import dev.praga.tracking.message.TrackingStatusUpdated;
import dev.praga.tracking.util.TestEventData;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.UUID.randomUUID;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.equalTo;


@Slf4j
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@ActiveProfiles("test")
@SpringBootTest(classes = TrackingConfiguration.class)
@EmbeddedKafka(controlledShutdown = true)
public class DispatchPreparingHandlerTest {

    private final static String TRACKING_STATUS_TOPIC = "tracking.status";
    private final static String DISPATCH_TRACKING_TOPIC= "dispatch.tracking";

    @Autowired
    private KafkaTestListener kafkaTestListener;

    @Autowired
    private KafkaTemplate kafkaTemplate;

    @Resource
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Resource
    private KafkaListenerEndpointRegistry registry;

    @Configuration
    static class TestConfig{
        @Bean
        public KafkaTestListener kafkaTestListner(){
            return new KafkaTestListener();
        }
    }
    public static class KafkaTestListener{
        AtomicInteger trackingStatus = new AtomicInteger(0);

        @KafkaListener(topics = TRACKING_STATUS_TOPIC, groupId = "KakfaTestListener")
        void testTrackingStatus(@Payload TrackingStatusUpdated statusUpdated){
            log.info("Tracking Status Updated:"+statusUpdated);
            trackingStatus.incrementAndGet();
        }
    }

    @BeforeEach
    public void setUp(){
        kafkaTestListener.trackingStatus.set(0);

        registry.getListenerContainers().stream().forEach(
                messageListenerContainer ->
                        ContainerTestUtils.waitForAssignment(messageListenerContainer, embeddedKafkaBroker.getPartitionsPerTopic())
        );
    }

    @Test
    void testTrackingFlowForDispatchPrepared() throws Exception{
        DispatchPrepared dispatchPrepared = TestEventData.createDispatchEvent(randomUUID());
        sendMessage(DISPATCH_TRACKING_TOPIC, dispatchPrepared);

        await().atMost(3, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
                .until(kafkaTestListener.trackingStatus::get, equalTo(1));
    }

    @Test
    void testTrackingFlowForDispatchCompleted() throws Exception{
        DispatchCompleted dispatchCompleted = TestEventData.createDispatchCompleted(randomUUID(), "2023-11-04");
        sendMessage(DISPATCH_TRACKING_TOPIC, dispatchCompleted);
        await().atMost(3, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
                .until(kafkaTestListener.trackingStatus::get, equalTo(1));
    }

    private void sendMessage(String trackingStatusTopic, Object data) throws Exception{
        kafkaTemplate.send(MessageBuilder
                .withPayload(data)
                .setHeader(KafkaHeaders.TOPIC, trackingStatusTopic)
                .build()).get();
    }
}
