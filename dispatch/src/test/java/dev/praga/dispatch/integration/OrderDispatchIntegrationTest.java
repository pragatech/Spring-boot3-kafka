package dev.praga.dispatch.integration;

import dev.praga.dispatch.DispatchConfiguration;
import dev.praga.dispatch.TestEventData;
import dev.praga.dispatch.message.DispatchCompleted;
import dev.praga.dispatch.message.DispatchPrepared;
import dev.praga.dispatch.message.OrderCreated;
import dev.praga.dispatch.message.OrderDispatched;
import jakarta.annotation.Resource;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.contract.wiremock.AutoConfigureWireMock;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaHandler;
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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.github.tomakehurst.wiremock.stubbing.Scenario.STARTED;
import static dev.praga.dispatch.integration.WiremockUtils.subWireMock;
import static java.util.UUID.randomUUID;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

@Slf4j
@AutoConfigureWireMock(port = 0)
@SpringBootTest(classes = DispatchConfiguration.class)
@ActiveProfiles("test")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@EmbeddedKafka(controlledShutdown = true)
public class OrderDispatchIntegrationTest {

    private final static String ORDER_CREATED_TOPIC="order.created";
    private final static String ORDER_DISPATCHED_TOPIC="order.dispatched";
    private final static String DISPATCH_TRACKING_TOPIC="dispatch.tracking";
    private final static String ORDER_CREATED_DTL_TOPIC = "order.created.DLT";

    //Autowire to use in all tests
    @Autowired
    private KafkaTestListner kafkaTestListner;

    @Autowired
    private KafkaTemplate kafkaTemplate;


    //make sure listener container bean have partition before start testing
    //@Autowired
    @Resource
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    //Intellij issue on autowired. but, code works with autowired
    //@Autowired
    @Resource
    private KafkaListenerEndpointRegistry registry;

    //Inorder to use, test listner, need to define spring context
    @Configuration
    static class testConfig{
        @Bean
        public KafkaTestListner kafkaTestListner(){
            return new KafkaTestListner();
        }
    }


    @KafkaListener(groupId = "kafkaIntegrationTest", topics = {DISPATCH_TRACKING_TOPIC,ORDER_DISPATCHED_TOPIC, ORDER_CREATED_DTL_TOPIC})
    //Defining Test Consumer
    public static class KafkaTestListner{
        AtomicInteger orderDispatchCounter = new AtomicInteger(0);
        AtomicInteger dispatchTrackingCounter = new AtomicInteger(0);
        AtomicInteger dispatchCompletedCounter = new AtomicInteger(0);

        AtomicInteger orderCreatedDLTCounter = new AtomicInteger(0);


        //===========================for handling Multiple Events=============================
        //@KafkaListener(groupId = "kafkaIntegrationTest", topics = DISPATCH_TRACKING_TOPIC)
        @KafkaHandler
        void receiveDispatchPrepared(@Payload DispatchPrepared dispatchPrepared){
            log.warn("dispatch Tracking:"+dispatchPrepared);
            dispatchTrackingCounter.incrementAndGet();
        }

        //@KafkaListener(groupId = "kafkaIntegrationTest", topics = DISPATCH_TRACKING_TOPIC)
        @KafkaHandler
        void receiveDispatchCompleted(@Payload DispatchCompleted dispatchCompleted){
            log.warn("dispatch Tracking:"+dispatchCompleted);
            dispatchCompletedCounter.incrementAndGet();
        }

        //@KafkaListener(groupId = "kafkaIntegrationTest", topics = ORDER_DISPATCHED_TOPIC)
        @KafkaHandler
        void receiveOrderDispatched(@Payload OrderDispatched orderDispatched){
            log.warn("orderDispatched Payload:"+orderDispatched);
            orderDispatchCounter.incrementAndGet();
        }

        @KafkaHandler
        void receiveOrderCreatedDTL(@Payload OrderCreated orderCreated){
            log.warn("orderDispatched Payload:"+orderCreated);
            orderCreatedDLTCounter.incrementAndGet();
        }
    }

    @BeforeEach
    public void setUp(){
        kafkaTestListner.dispatchTrackingCounter.set(0);
        kafkaTestListner.orderDispatchCounter.set(0);
        kafkaTestListner.dispatchCompletedCounter.set(0);
        kafkaTestListner.orderCreatedDLTCounter.set(0);
        WiremockUtils.reset();

        registry.getListenerContainers().stream().forEach(
                container ->
                        ContainerTestUtils.waitForAssignment(container,
                                container.getContainerProperties().getTopics().length *
                                embeddedKafkaBroker.getPartitionsPerTopic())
        );
    }

    @Test
    public void testOrderDispatchFlow_success() throws Exception{
        subWireMock("/api/stock?item=my-item", 200, "true");
        String key = randomUUID().toString();
        OrderCreated orderCreated = TestEventData.buildOrderCreatedEvent(
                randomUUID(), "my-item"
        );
        sendMessage(ORDER_CREATED_TOPIC, key, orderCreated);
        await().atMost(5, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
                .until(kafkaTestListner.dispatchTrackingCounter::get, equalTo(1));
        await().atMost(3, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
                .until(kafkaTestListner.orderDispatchCounter::get, equalTo(1));
        await().atMost(3, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
                .until(kafkaTestListner.dispatchCompletedCounter::get, equalTo(1));
        assertThat(kafkaTestListner.orderCreatedDLTCounter.get(), equalTo(0));
    }

    @Test
    public void testOrderDispatchFlow_RetryableException() throws Exception{
        subWireMock("/api/stock?item=my-item", 503, "Server unavailable",
                "failOnce", STARTED, "succeedNextTime");
        subWireMock("/api/stock?item=my-item", 200, "true",
                "failOnce","succeedNextTime", "succeedNextTime");
        String key = randomUUID().toString();
        OrderCreated orderCreated = TestEventData.buildOrderCreatedEvent(
                randomUUID(), "my-item"
        );
        sendMessage(ORDER_CREATED_TOPIC, key, orderCreated);
        await().atMost(3, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
                .until(kafkaTestListner.dispatchTrackingCounter::get, equalTo(1));
        await().atMost(3, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
                .until(kafkaTestListner.orderDispatchCounter::get, equalTo(1));
        await().atMost(3, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
                .until(kafkaTestListner.dispatchCompletedCounter::get, equalTo(1));
        assertThat(kafkaTestListner.orderCreatedDLTCounter.get(), equalTo(0));
    }


    @Test
    public void testOrderDispatchFlow_NonRetryableException() throws Exception {
        subWireMock("/api/stock?item=my-item", 400, "Bad Request");
        String key = randomUUID().toString();
        OrderCreated orderCreated = TestEventData.buildOrderCreatedEvent(
                randomUUID(), "my-item"
        );
        sendMessage(ORDER_CREATED_TOPIC, key, orderCreated);
        await().atMost(3, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
                        .until(kafkaTestListner.orderCreatedDLTCounter::get, equalTo(1));
        assertThat(kafkaTestListner.dispatchCompletedCounter.get(), equalTo(0));
        assertThat(kafkaTestListner.dispatchCompletedCounter.get(), equalTo(0));
        assertThat(kafkaTestListner.dispatchTrackingCounter.get(), equalTo(0));
    }

    @Test
    public void testOrderDispatchFlow_RetryUntilFailure() throws Exception {
        subWireMock("/api/stock?item=my-item", 503, "Server unavailable");
        String key = randomUUID().toString();
        OrderCreated orderCreated = TestEventData.buildOrderCreatedEvent(
                randomUUID(), "my-item"
        );
        sendMessage(ORDER_CREATED_TOPIC, key, orderCreated);
        await().atMost(5, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS)
                .until(kafkaTestListner.orderCreatedDLTCounter::get, equalTo(1));
        assertThat(kafkaTestListner.dispatchCompletedCounter.get(), equalTo(0));
        assertThat(kafkaTestListner.dispatchCompletedCounter.get(), equalTo(0));
        assertThat(kafkaTestListner.dispatchTrackingCounter.get(), equalTo(0));
    }

    private void sendMessage(String orderCreatedTopic, String key, Object data) throws ExecutionException, InterruptedException {
        kafkaTemplate.send(MessageBuilder
                .withPayload(data)
                .setHeader(KafkaHeaders.TOPIC, ORDER_CREATED_TOPIC)
                .setHeader(KafkaHeaders.KEY, key)
                .build()).get();
    }
}
