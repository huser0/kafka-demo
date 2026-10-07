package io.huser0.consumer.service;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumerService {
    @KafkaListener(
            topics = "${app.topic.name}",
            groupId = "${spring.kafka.consumer.group-id}"
    )
    public void listen(String message){
        System.out.println("Mensagem recebida: " + message);
    }
}
