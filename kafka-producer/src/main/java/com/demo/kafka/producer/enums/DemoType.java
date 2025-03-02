package com.demo.kafka.producer.enums;

public enum DemoType {
    WITH_MESSAGE_KEY,
    WITHOUT_MESSAGE_KEY,
    TO_SPECIFIC_PARTITION,
    WITH_CUSTOM_PARTITIONER
}
