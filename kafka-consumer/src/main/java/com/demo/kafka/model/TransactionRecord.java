package com.demo.kafka.model;

import com.demo.kafka.consumer.enums.PaymentMode;
import com.demo.kafka.consumer.enums.TransactionType;
import lombok.*;

@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
@ToString
public class TransactionRecord {
    private  TransactionType transactionType;
    private  PaymentMode paymentMode;
    private  String txnTime;
    private  float amount;
    private  int txnSeqNo;
    private  String txnNarrative;

}
