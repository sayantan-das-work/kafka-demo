package com.demo.kafka.consumer.service;

import com.demo.kafka.model.TransactionRecord;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class TransactionConsumer {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Autowired
    private KafkaTemplate<Integer, TransactionRecord> kafkaTemplate;

    @Autowired
    private String kafkaTopicName;


    /*@KafkaListener(
            topics = {"Demo-Topic-2"},
            groupId = "demo-consumer-grp-1",
            containerFactory = "concurrentKafkaListenerContainerFactory"
    )*/
    public void consumeTransactionEvent(TransactionRecord transactionRecord){
        try{
            log.info("Consuming Txn Seq No.-{} || Transaction event {} || kafka topic {}....",
                    transactionRecord.getTxnSeqNo(),
                    transactionRecord,
                    kafkaTopicName
            );

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @KafkaListener(
            topics = {"Demo-Topic-2"},
            groupId = "demo-consumer-grp-1",
            containerFactory = "concurrentKafkaListenerContainerFactory"
    )
    public void consumeStringTransactionEvent(ConsumerRecord<Integer, TransactionRecord> txnConsumerRecord){
        try{
            TransactionRecord transactionRecordConsumed = txnConsumerRecord.value();
            log.info("Consuming Txn Seq No.-{} || String Transaction event {} || kafka topic : {} || partition no. : {} || key : {}",
                    transactionRecordConsumed.getTxnSeqNo(),
                    transactionRecordConsumed,
                    txnConsumerRecord.topic(),
                    txnConsumerRecord.partition(),
                    txnConsumerRecord.key()
            );

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
