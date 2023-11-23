package dev.praga.dispatch.handler;

import dev.praga.dispatch.TestEventData;
import dev.praga.dispatch.exception.NonRetryableException;
import dev.praga.dispatch.message.OrderCreated;
import dev.praga.dispatch.service.DispatchService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static java.util.UUID.randomUUID;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class OrderCreatedHandlerTest {

    private DispatchService dispatchServiceMock;
    private OrderCreatedHandler orderCreatedHandler;

    @BeforeEach
    void setUp(){
        dispatchServiceMock = mock(DispatchService.class);
        orderCreatedHandler = new OrderCreatedHandler(dispatchServiceMock);
    }

    @Test
    void listen_success() throws Exception{
        String key = randomUUID().toString();
        OrderCreated testEvent = TestEventData.buildOrderCreatedEvent(randomUUID(), randomUUID().toString());
        orderCreatedHandler.listen(0, key, testEvent);
        verify(dispatchServiceMock, times(1)).process(key, testEvent);
    }

    @Test
    void listen_service() throws Exception{
        String key = randomUUID().toString();
        OrderCreated testEvent = TestEventData.buildOrderCreatedEvent(randomUUID(), randomUUID().toString());
        doThrow(new RuntimeException("Service failure"))
                .when(dispatchServiceMock)
                .process(key, testEvent);

        Exception exception = assertThrows(NonRetryableException.class,
                () -> orderCreatedHandler.listen(0,key, testEvent));
        assertThat(exception.getMessage(),
                equalTo("java.lang.RuntimeException: Service failure"));
        verify(dispatchServiceMock, times(1)).process(key, testEvent);
    }
}