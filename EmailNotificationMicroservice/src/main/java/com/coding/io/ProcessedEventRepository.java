package com.coding.io;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface ProcessedEventRepository extends JpaRepository<ProcessedEventEntity,Long> {
    ProcessedEventEntity findByMessageId(String messageId);
}
