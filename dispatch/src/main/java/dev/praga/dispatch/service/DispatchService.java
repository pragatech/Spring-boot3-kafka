package dev.praga.dispatch.service;

import dev.praga.dispatch.client.StockServiceClient;
import dev.praga.dispatch.message.DispatchCompleted;
import dev.praga.dispatch.message.DispatchPrepared;
import dev.praga.dispatch.message.OrderCreated;
import dev.praga.dispatch.message.OrderDispatched;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;
import java.time.format.FormatStyle;
import java.util.Calendar;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static java.util.UUID.randomUUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class DispatchService {

    private final UUID APPLICATION_ID = randomUUID();
    private final String ORDER_DISPATCH_TOPIC = "order.dispatched";
    private final String DISPATCH_TRACKING_TOPIC = "dispatch.tracking";
    private final KafkaTemplate<String, Object> kafkaProducer;

    private final StockServiceClient stockServiceClient;

    public void process(String key, OrderCreated payload) throws Exception {
        String available = stockServiceClient.checkAvailability(payload.getItem());
        if(Boolean.valueOf(available)) {
            DispatchPrepared dispatchPrepared = DispatchPrepared.builder()
                    .orderId(payload.getOrderId())
                    .build();
            kafkaProducer.send(DISPATCH_TRACKING_TOPIC, key, dispatchPrepared).get();

            log.info("key:" + key + " - topic:" + DISPATCH_TRACKING_TOPIC + " - Payload:" + dispatchPrepared);
            OrderDispatched orderDispatched = OrderDispatched.builder()
                    .orderId(payload.getOrderId())
                    .processById(APPLICATION_ID)
                    .notes("Event Message: " + payload.getOrderId() + " - ProcessID:" + APPLICATION_ID)
                    .build();
            kafkaProducer.send(ORDER_DISPATCH_TOPIC, key, orderDispatched).get();

            log.info("key:" + key + " - topic:" + ORDER_DISPATCH_TOPIC + " - Payload:" + orderDispatched);

            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
            DispatchCompleted dispatchCompleted = DispatchCompleted.builder()
                    .orderId(payload.getOrderId())
                    .date(simpleDateFormat.format(Calendar.getInstance().getTime()))
                    .build();
            kafkaProducer.send(DISPATCH_TRACKING_TOPIC, key, dispatchCompleted).get();
            log.info("key:" + key + " - payload - DC:" + dispatchCompleted);
        }else {
            log.info("item "+payload.getItem()+" is unavailable");
        }
    }
}
