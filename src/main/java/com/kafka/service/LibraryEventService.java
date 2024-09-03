package com.kafka.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.entity.LibraryEvent;
import com.kafka.repository.LibraryEventRepo;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class LibraryEventService {

    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    LibraryEventRepo libraryEventRepo;

    public void processLibraryEvent(final ConsumerRecord<Integer, String> record) throws JsonProcessingException {
        var libraryEvent = objectMapper.readValue(record.value(), LibraryEvent.class);

        if (libraryEvent!= null && libraryEvent.getLibraryEventId()!= null && libraryEvent.getLibraryEventId() == 999)
            throw new RecoverableDataAccessException("Connection issue");

        switch (libraryEvent.getLibraryEventType()) {
            case NEW -> {
                save(libraryEvent);
                break;
            }
            case UPDATE -> {
                validate(libraryEvent);
                save(libraryEvent);
                break;
            }
            default -> log.info("Library event type is not supported");
        }

    }

    private void validate(LibraryEvent libraryEvent) {
        if (libraryEvent.getLibraryEventId() == null) {
            throw new IllegalArgumentException("Library event id is null");
        }

        var libraryEventOptional = libraryEventRepo.findById(libraryEvent.getLibraryEventId());
        log.info("Library event found: {}", libraryEventOptional);
        if (libraryEventOptional.isEmpty()) {
            throw new IllegalArgumentException("Not a valid library event id");
        }
        log.info("Validation is successful for library event: {}", libraryEventOptional.get());
    }

    private void save(LibraryEvent libraryEvent) {
        libraryEvent.getBook().setLibraryEvent(libraryEvent);
        libraryEventRepo.save(libraryEvent);
    }
}
