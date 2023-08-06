package com.goldsilver.service;

import com.goldsilver.dto.Customer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.service.annotation.PostExchange;

import java.util.concurrent.CompletableFuture;

@Service
public class KafkaMessagePublisher {

    private final KafkaTemplate<String, Object> template;

    @Autowired
    public KafkaMessagePublisher(KafkaTemplate<String, Object> template) {
        this.template = template;
    }

    public void sendMessageToTopic(String message) {
        CompletableFuture<SendResult<String, Object>> future = template.send("goldsilver-success", message);
        future.whenComplete((result, ex) -> {
                    if (ex == null) {
                        System.out.println("Sent message=[" + message +
                                "] with offset=[" + result.getRecordMetadata().offset() + "]");
                    } else {
                        System.out.println("Unable to send message=[" +
                                message + "] due to: " + ex.getMessage());
                    }
                }
        );
    }


    public void sendEventsToTopic(Customer customer) {
        try {
            CompletableFuture<SendResult<String, Object>> future = template.send("goldsilver-event-topic", customer);
            future.whenComplete((result, ex) -> {
                        if (ex == null) {
                            System.out.println("Sent message=[" + customer +
                                    "] with offset=[" + result.getRecordMetadata().offset() + "]");
                        } else {
                            System.out.println("Unable to send message=[" +
                                    customer + "] due to: " + ex.getMessage());
                        }
                    }
            );
        }
        catch (Exception exception){
            System.out.println("Error with a message:" + exception.getMessage());
        }

    }
}
