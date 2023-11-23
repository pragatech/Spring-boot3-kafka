package dev.praga.tracking.util;

import dev.praga.tracking.message.DispatchCompleted;
import dev.praga.tracking.message.DispatchPrepared;

import java.util.UUID;

public class TestEventData {

    public static DispatchPrepared createDispatchEvent(UUID orderId){
        DispatchPrepared dispatchPrepared = DispatchPrepared.builder()
                .orderId(orderId)
                .build();
        return dispatchPrepared;
    }

    public static DispatchCompleted createDispatchCompleted(UUID orderId, String date){
        return DispatchCompleted.builder()
                .orderId(orderId)
                .dispatchedDate(date)
                .build();
    }
}
