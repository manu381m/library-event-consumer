package com.kafka.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.kafka.service.LibraryEventService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class LibraryEventsConsumer {

    @Autowired
    private LibraryEventService libraryEventService;

    @KafkaListener(topics = "library-events", groupId = "library-events-consumer-group")
    public void listen(final ConsumerRecord<Integer, String> record) throws JsonProcessingException {
        log.info("Consumer record: {}", record);
        libraryEventService.processLibraryEvent(record);
    }
}
