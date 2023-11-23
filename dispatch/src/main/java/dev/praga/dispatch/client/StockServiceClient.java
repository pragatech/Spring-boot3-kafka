package dev.praga.dispatch.client;

import dev.praga.dispatch.exception.RetryableException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;

@Slf4j
@Component
public class StockServiceClient {
    private final RestTemplate restTemplate;

    private final String stockServiceClient;

    public StockServiceClient(
            @Autowired RestTemplate restTemplate,
            @Value("${dispatch.stockServiceEndpoint}") String stockServiceClient) {
        this.restTemplate = restTemplate;
        this.stockServiceClient = stockServiceClient;
    }

    public String checkAvailability(String item){
        try{
            ResponseEntity<String> responseEntity = restTemplate
                    .getForEntity(stockServiceClient+"?item="+item, String.class);
            if(responseEntity.getStatusCodeValue() != 200){
                throw new RuntimeException("error " + responseEntity.getStatusCodeValue());
            }
            return responseEntity.getBody();
        }catch (HttpServerErrorException | ResourceAccessException e){
            log.warn("Failure calling external service", e);
            throw new RetryableException(e);
        }catch (Exception e){
            log.error("Exception thrown: "+e.getClass().getName(), e);
            throw e;
        }
    }
}
