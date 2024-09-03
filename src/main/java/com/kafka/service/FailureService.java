package com.kafka.service;

import com.kafka.entity.FailureRecord;
import com.kafka.repository.FailureRecordRepo;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class FailureService {

    private final FailureRecordRepo failureRecordRepo;

    public FailureService(final FailureRecordRepo failureRecordRepo) {
        this.failureRecordRepo = failureRecordRepo;
    }

    public void saveFailureRecord(ConsumerRecord<Integer, String> consumerRecord, Exception e, String errorStatus) {
        log.info("Manoj");
        var failureRecord = new FailureRecord(null, consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset(), consumerRecord.key(), consumerRecord.value(), e.getCause().getMessage(), errorStatus);
        log.info("Manoj");
        failureRecordRepo.save(failureRecord);
        log.info("Manoj");
    }
}
