package com.kafka.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

//@Service
@Slf4j
public class LibraryEventsConsumerManualOffset implements AcknowledgingMessageListener {


    @Override
    @KafkaListener(topics = "library-events")
    public void onMessage(ConsumerRecord data, Acknowledgment acknowledgment) {
        log.info("Received a record with key: {}, value: {}", data.key(), data.value());
        acknowledgment.acknowledge();
    }

    @Override
    public void onMessage(Object data) {

    }
}
