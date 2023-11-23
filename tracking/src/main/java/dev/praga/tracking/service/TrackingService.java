package dev.praga.tracking.service;

import dev.praga.tracking.message.DispatchCompleted;
import dev.praga.tracking.message.DispatchPrepared;
import dev.praga.tracking.message.TrackingStatusUpdated;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.concurrent.ExecutionException;

@Service
@RequiredArgsConstructor
public class TrackingService {

    private final String TRACKING_STATUS_TOPIC = "tracking.status";
    private final KafkaTemplate<String, Object> kafkaTemplate;
    public void process(DispatchPrepared payload) throws Exception {
        TrackingStatusUpdated statusUpdated = TrackingStatusUpdated.builder()
                        .orderId(payload.getOrderId())
                        .status(TrackingStatus.PREPARING).build();

        kafkaTemplate.send(TRACKING_STATUS_TOPIC, statusUpdated).get();
    }

    public void process(DispatchCompleted dispatchCompleted) throws Exception {
        TrackingStatusUpdated statusUpdated = TrackingStatusUpdated.builder()
                .orderId(dispatchCompleted.getOrderId())
                .status(TrackingStatus.COMPLETED)
                .build();
        kafkaTemplate.send(TRACKING_STATUS_TOPIC, statusUpdated).get();
    }
}
