package dev.praga.dispatch.client;

import dev.praga.dispatch.exception.RetryableException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.http.HttpStatusCode;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

public class StockServiceClientTest {

    private RestTemplate restTemplateMock;
    private StockServiceClient stockServiceClientMock;

    private final String STOCK_SERVICE_ENDPOINT = "endpoint";
    private final String STOCK_SERVICE_QUERY = STOCK_SERVICE_ENDPOINT + "?item=my-item";

    @BeforeEach
    public void setUp(){
        restTemplateMock = mock(RestTemplate.class);
        stockServiceClientMock = new StockServiceClient(restTemplateMock, STOCK_SERVICE_ENDPOINT);
    }

    @Test
    public void checkAvailability_success(){
        ResponseEntity<String> response = new ResponseEntity<>("true", HttpStatusCode.valueOf(200));
        when(restTemplateMock.getForEntity(STOCK_SERVICE_QUERY, String.class)).thenReturn(response);
        assertThat(stockServiceClientMock.checkAvailability("my-item"), equalTo("true"));
        verify(restTemplateMock, times(1)).getForEntity(STOCK_SERVICE_QUERY, String.class);
    }

    @Test
    public void checkAvailability_serverException(){
        doThrow(new HttpServerErrorException(HttpStatusCode.valueOf(500)))
                .when(restTemplateMock).getForEntity(STOCK_SERVICE_QUERY, String.class);
        assertThrows(RetryableException.class, () -> stockServiceClientMock.checkAvailability("my-item"));
        verify(restTemplateMock, times(1)).getForEntity(STOCK_SERVICE_QUERY, String.class);
    }

    @Test
    public void checkAvailability_ResourceAccessException(){
        doThrow(new ResourceAccessException("Access Exception")).when(restTemplateMock).getForEntity(STOCK_SERVICE_QUERY, String.class);
        assertThrows(RetryableException.class, () -> stockServiceClientMock.checkAvailability("my-item"));
        verify(restTemplateMock, times(1)).getForEntity(STOCK_SERVICE_QUERY, String.class);
    }

    @Test
    public void checkAvailability_Exception(){
        doThrow(new RuntimeException("General Exception")).when(restTemplateMock).getForEntity(STOCK_SERVICE_QUERY, String.class);
        assertThrows(Exception.class, () -> stockServiceClientMock.checkAvailability("my-item"));
        verify(restTemplateMock, times(1)).getForEntity(STOCK_SERVICE_QUERY, String.class);
    }
}
