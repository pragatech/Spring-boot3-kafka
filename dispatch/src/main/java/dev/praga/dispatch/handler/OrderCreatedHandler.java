package dev.praga.dispatch.handler;

import dev.praga.dispatch.exception.NonRetryableException;
import dev.praga.dispatch.exception.RetryableException;
import dev.praga.dispatch.message.OrderCreated;
import dev.praga.dispatch.service.DispatchService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutionException;

@Slf4j
@RequiredArgsConstructor
@Component
public class OrderCreatedHandler {

    private final DispatchService dispatchService;

    @KafkaListener(
            id = "orderConsumerClient",
            topics = "order.created",
            groupId = "dispatch.order.created.consumer",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void listen(
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.RECEIVED_KEY) String key,
            @Payload OrderCreated payload
    ){
        log.warn("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%");
        log.warn("Key:"+ key+" - Partition:"+partition);
        log.warn("Received Message: Payload="+payload);
        try {
            dispatchService.process(key, payload);
        } catch (RetryableException e) {
            log.warn("Retryable Exception", e);
            throw e;
        } catch (Exception e){
            log.error("NonRetryable Exception", e);
            throw new NonRetryableException(e);
        }
    }
}
