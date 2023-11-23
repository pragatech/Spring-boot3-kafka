package dev.praga.tracking.handler;

import dev.praga.tracking.message.DispatchCompleted;
import dev.praga.tracking.util.TestEventData;
import dev.praga.tracking.message.DispatchPrepared;
import dev.praga.tracking.service.TrackingService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static java.util.UUID.randomUUID;
import static org.mockito.Mockito.*;

class DispatchHandlerTest {

    private TrackingService trackingServiceMock;
    private DispatchHandler dispatchHandler;

    @BeforeEach
    void setUp()
    {
        trackingServiceMock = mock(TrackingService.class);
        dispatchHandler = new DispatchHandler(trackingServiceMock);
    }

    @Test
    void listen_dispatch_prepared_success() throws Exception{
        DispatchPrepared dispatchPrepared = TestEventData.createDispatchEvent(randomUUID());
        dispatchHandler.listen(dispatchPrepared);
        verify(trackingServiceMock, times(1)).process(dispatchPrepared);
    }

    @Test
    void listen_dispatch_prepared_service() throws Exception{
        DispatchPrepared dispatchPrepared = TestEventData.createDispatchEvent(randomUUID());
        doThrow(new RuntimeException("process failure"))
                .when(trackingServiceMock)
                .process(dispatchPrepared);
        dispatchHandler.listen(dispatchPrepared);
        verify(trackingServiceMock, times(1)).process(dispatchPrepared);
    }

    @Test
    void listen_dispatch_completed_success() throws Exception{
        DispatchCompleted dispatchCompleted = TestEventData.createDispatchCompleted(randomUUID(), "2023-11-04");
        dispatchHandler.listen(dispatchCompleted);
        verify(trackingServiceMock,times(1)).process(dispatchCompleted);
    }

    @Test
    void listen_dispatch_completed_service() throws Exception{
        DispatchCompleted dispatchCompleted = TestEventData.createDispatchCompleted(randomUUID(), "2023-11-04");
        doThrow(new RuntimeException("Dispatch Completed Error"))
                .when(trackingServiceMock)
                .process(any(DispatchCompleted.class));
        dispatchHandler.listen(dispatchCompleted);
        verify(trackingServiceMock, times(1)).process(dispatchCompleted);
    }
}