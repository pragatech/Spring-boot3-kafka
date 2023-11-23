package dev.praga.dispatch.service;

import dev.praga.dispatch.TestEventData;
import dev.praga.dispatch.client.StockServiceClient;
import dev.praga.dispatch.message.DispatchCompleted;
import dev.praga.dispatch.message.DispatchPrepared;
import dev.praga.dispatch.message.OrderCreated;
import dev.praga.dispatch.message.OrderDispatched;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.concurrent.CompletableFuture;

import static java.util.UUID.randomUUID;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

class DispatchServiceTest {
    private DispatchService dispatchService;
    private KafkaTemplate kafkaTemplateMock;
    private StockServiceClient stockServiceClientMock;

    @BeforeEach
    void setUp() {
        kafkaTemplateMock = mock(KafkaTemplate.class);
        stockServiceClientMock = mock(StockServiceClient.class);
        dispatchService = new DispatchService(kafkaTemplateMock, stockServiceClientMock);
    }

    @Test
    void process_success() throws Exception{
        when(kafkaTemplateMock.send(anyString(), anyString(), any(OrderDispatched.class))).thenReturn(mock(CompletableFuture.class));
        when(kafkaTemplateMock.send(anyString(), anyString(), any(DispatchPrepared.class))).thenReturn(mock(CompletableFuture.class));
        when(kafkaTemplateMock.send(anyString(), anyString(), any(DispatchCompleted.class))).thenReturn(mock(CompletableFuture.class));
        when(stockServiceClientMock.checkAvailability(anyString())).thenReturn("true");
        String key = randomUUID().toString();
        OrderCreated testEvent = TestEventData.buildOrderCreatedEvent(randomUUID(), randomUUID().toString());
        dispatchService.process(key, testEvent);

        verify(kafkaTemplateMock, times(1)).send(eq("dispatch.tracking"), eq(key),any(DispatchPrepared.class));
        verify(kafkaTemplateMock, times(1)).send(eq("order.dispatched"), eq(key),any(OrderDispatched.class));
        verify(kafkaTemplateMock,times(1)).send(eq("dispatch.tracking"), eq(key), any(DispatchCompleted.class));
        verify(stockServiceClientMock, times(1)).checkAvailability(testEvent.getItem());
    }

    @Test
    void process_DispatchTrackingProducerthrowsException() throws Exception{
        String key = randomUUID().toString();
        OrderCreated testEvent = TestEventData.buildOrderCreatedEvent(randomUUID(), randomUUID().toString());
        when(stockServiceClientMock.checkAvailability(anyString())).thenReturn("true");
        doThrow(new RuntimeException("Dispatch Tracking Producer Failure"))
                .when(kafkaTemplateMock)
                .send(eq("dispatch.tracking"), eq(key), any(DispatchPrepared.class));

        Exception exception = assertThrows(RuntimeException.class, () -> dispatchService.process(key, testEvent));
        verify(stockServiceClientMock, times(1)).checkAvailability(testEvent.getItem());
        verify(kafkaTemplateMock, times(1)).send(eq("dispatch.tracking"), eq(key), any(DispatchPrepared.class));
        verifyNoMoreInteractions(kafkaTemplateMock);
        assertThat(exception.getMessage(), equalTo("Dispatch Tracking Producer Failure"));
    }

    @Test
    void process_OrderDispatchedProducerthrowsException() throws Exception{
        String key = randomUUID().toString();
        OrderCreated testEvent = TestEventData
                .buildOrderCreatedEvent(randomUUID(), randomUUID().toString());
        when(stockServiceClientMock.checkAvailability(anyString())).thenReturn("true");
        when(kafkaTemplateMock.send(anyString(), anyString(), any(DispatchPrepared.class)))
                .thenReturn(mock(CompletableFuture.class));
        doThrow(new RuntimeException("Order Dispatch Producer Failure"))
                .when(kafkaTemplateMock)
                .send(eq("order.dispatched"), eq(key), any(OrderDispatched.class));

        Exception exception = assertThrows(RuntimeException.class, () -> dispatchService.process(key, testEvent));
        verify(stockServiceClientMock, times(1)).checkAvailability(testEvent.getItem());
        verify(kafkaTemplateMock, times(1)).send(eq("order.dispatched"), eq(key),any(OrderDispatched.class));
        assertThat(exception.getMessage(), equalTo("Order Dispatch Producer Failure"));
    }

    @Test
    void process_OrderDispatchCompletedProducerthrowsException() throws Exception{
        String key = randomUUID().toString();
        OrderCreated orderCreated = TestEventData.buildOrderCreatedEvent(
                randomUUID(), randomUUID().toString()
        );
        when(stockServiceClientMock.checkAvailability(anyString())).thenReturn("true");
        when(kafkaTemplateMock.send(anyString(), anyString(), any(DispatchPrepared.class))).thenReturn(mock(CompletableFuture.class));
        when(kafkaTemplateMock.send(anyString(), anyString(), any(OrderDispatched.class))).thenReturn(mock(CompletableFuture.class));
        doThrow(new RuntimeException("Dispatch Completed error"))
                .when(kafkaTemplateMock)
                .send(eq("dispatch.tracking"), anyString(), any(DispatchCompleted.class));
        Exception exception = assertThrows(RuntimeException.class, () -> dispatchService.process(key, orderCreated));
        verify(stockServiceClientMock, times(1)).checkAvailability(orderCreated.getItem());
        verify(kafkaTemplateMock, times(1)).send(eq("dispatch.tracking"), eq(key), any(DispatchPrepared.class));
        verify(kafkaTemplateMock, times(1)).send(eq("order.dispatched"), eq(key), any(OrderDispatched.class));
        verify(kafkaTemplateMock, times(1)).send(eq("dispatch.tracking"), eq(key), any(DispatchCompleted.class));
        assertThat(exception.getMessage(), equalTo("Dispatch Completed error"));
    }
}