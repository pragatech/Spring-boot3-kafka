package dev.praga.dispatch;

import dev.praga.dispatch.message.OrderCreated;
import scala.Int;

import java.util.UUID;

public class TestEventData {
    public static OrderCreated buildOrderCreatedEvent(UUID orderId, String item){
        return OrderCreated.builder()
                .orderId(orderId)
                .item(item)
                .build();
    }
}
