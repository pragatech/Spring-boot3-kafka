package dev.praga.tracking.handler;

import dev.praga.tracking.message.DispatchCompleted;
import dev.praga.tracking.message.DispatchPrepared;
import dev.praga.tracking.service.TrackingService;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@AllArgsConstructor
@Component
@KafkaListener(
        id="dispatchConsumerClient",
        topics = "dispatch.tracking",
        groupId = "tracker.dispatch.tracking.consumer",
        containerFactory = "kafkaListenerContainerFactory"
)
public class DispatchHandler {

    private final TrackingService trackingService;

    @KafkaHandler
    public void listen(DispatchPrepared payload){
        log.warn("##########################");
        log.warn("PayLoad:"+payload);
        try {
            trackingService.process(payload);
        } catch (Exception e) {
            log.error("Processing Failure!", e);
        }
    }

    @KafkaHandler
    public void listen(DispatchCompleted dispatchCompleted){
        log.warn("$$$$$$$$$");
        log.warn("Payload:"+dispatchCompleted);
        try {
            trackingService.process(dispatchCompleted);
        } catch (Exception e) {
            log.error("Producer failure!", e);
        }
    }
}
