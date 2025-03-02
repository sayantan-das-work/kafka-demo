package com.demo.kafka.model;

import com.demo.kafka.producer.enums.PaymentMode;
import com.demo.kafka.producer.enums.TransactionType;

public record TransactionRecord(
    TransactionType transactionType,
    PaymentMode paymentMode,
    String txnTime,
    float amount,
    int txnSeqNo,
    String txnNarrative){ }
