package dev.praga.tracking.service;

import dev.praga.tracking.message.DispatchCompleted;
import dev.praga.tracking.util.TestEventData;
import dev.praga.tracking.message.DispatchPrepared;
import dev.praga.tracking.message.TrackingStatusUpdated;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.concurrent.CompletableFuture;

import static  java.util.UUID.randomUUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class TrackingServiceTest {

    private KafkaTemplate kafkaTemplateMock;
    private TrackingService trackingService;

    @BeforeEach
    void setUp() {
        kafkaTemplateMock = mock(KafkaTemplate.class);
        trackingService = new TrackingService(kafkaTemplateMock);
    }

    @Test
    void process_dispatch_preparing_success() throws Exception{
        DispatchPrepared dispatchPrepared = TestEventData.createDispatchEvent(randomUUID());
        when(kafkaTemplateMock.send(anyString(), any(TrackingStatusUpdated.class)))
                .thenReturn(mock(CompletableFuture.class));
        trackingService.process(dispatchPrepared);
        verify(kafkaTemplateMock, times(1))
                .send(
                        eq("tracking.status"),
                        any(TrackingStatusUpdated.class)
                );
    }

    @Test
    void process_dispatch_preparing_service() throws Exception{
        DispatchPrepared dispatchPrepared = TestEventData.createDispatchEvent(randomUUID());
        doThrow(new RuntimeException("Producer Failure"))
                .when(kafkaTemplateMock)
                .send(anyString(),any(TrackingStatusUpdated.class));
        Exception exception =assertThrows(
                RuntimeException.class,
                () -> trackingService.process(dispatchPrepared)
        );
        verify(kafkaTemplateMock, times(1)).send(
                eq("tracking.status"),
                any(TrackingStatusUpdated.class)
        );
        assertThat(exception.getMessage(), equalTo("Producer Failure"));
    }

    @Test
    void process_dispatch_completed_success() throws Exception{
        DispatchCompleted dispatchCompleted = TestEventData.createDispatchCompleted(randomUUID(), "2023-11-04");
        when(kafkaTemplateMock.send(anyString(), any(TrackingStatusUpdated.class)))
                .thenReturn(mock(CompletableFuture.class));
        trackingService.process(dispatchCompleted);
        verify(kafkaTemplateMock, times(1))
                .send(eq("tracking.status"), any(TrackingStatusUpdated.class));
    }

    @Test
    public void process_dispatch_completed_throwException() throws Exception{
        DispatchCompleted dispatchCompleted = TestEventData.createDispatchCompleted(randomUUID(), "2023-11-04");
        doThrow(new RuntimeException("Dispatch Completed Error!"))
                .when(kafkaTemplateMock)
                .send(anyString(), any(TrackingStatusUpdated.class));
        Exception exception = assertThrows(RuntimeException.class, () -> trackingService.process(dispatchCompleted));
        verify(kafkaTemplateMock, times(1))
                .send(eq("tracking.status"), any(TrackingStatusUpdated.class));
        assertThat(exception.getMessage(), equalTo("Dispatch Completed Error!"));
    }
}