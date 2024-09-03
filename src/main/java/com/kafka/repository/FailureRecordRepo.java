package com.kafka.repository;

import com.kafka.entity.FailureRecord;
import org.springframework.data.repository.CrudRepository;

import java.util.List;

public interface FailureRecordRepo extends CrudRepository<FailureRecord, Integer> {
    List<FailureRecord> findAllByErrorStatus(String errorStatus);
}
