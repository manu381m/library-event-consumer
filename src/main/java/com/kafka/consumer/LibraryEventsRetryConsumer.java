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
public class LibraryEventsRetryConsumer {

    @Autowired
    private LibraryEventService libraryEventService;

    @KafkaListener(topics = "${topics.retry}", autoStartup = "${retryListener.startup:false}", groupId = "library-events-retry-group")
    public void onMessage(final ConsumerRecord<Integer, String> record) throws JsonProcessingException {
        log.info("Retry consumer record: {}", record);
        record.headers().forEach(header -> {
            log.info("Retry header " + header.key() + ": " + new String(header.value()));
        });

        libraryEventService.processLibraryEvent(record);
    }
}
