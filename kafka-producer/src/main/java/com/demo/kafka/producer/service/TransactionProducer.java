package com.demo.kafka.producer.service;

import com.demo.kafka.model.TransactionRecord;
import com.demo.kafka.producer.ProduceException;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.common.utils.Utils.sleep;

@Service
@Slf4j
public class TransactionProducer {
    @Autowired
    @Qualifier("kafkaTemplateWithoutPartitioner")
    private KafkaTemplate<Integer, TransactionRecord> kafkaTemplateWithoutPartitoner;

    @Autowired
    @Qualifier("kafkaTemplateWithCustomPartitioner")
    private KafkaTemplate<Integer, TransactionRecord> kafkaTemplateWithPartitioner;

    @Autowired
    private String kafkaTopicName;


    public void publishTransactionEventWithoutKey(@Nonnull TransactionRecord transactionRecord) throws ProduceException {
        produceTransactionHappenEventMessageKeyDemo(
                kafkaTemplateWithoutPartitoner,
                null,
                transactionRecord);
    }

    public void publishTransactionEventWithKey(@Nonnull TransactionRecord transactionRecord) throws ProduceException {
        produceTransactionHappenEventMessageKeyDemo(
                kafkaTemplateWithoutPartitoner,
                transactionRecord.txnSeqNo(),
                transactionRecord);
    }

    public void publishTransactionEventCustomPartition(@Nonnull TransactionRecord transactionRecord) throws ProduceException {
        produceTransactionHappenEventMessageKeyDemo(
                kafkaTemplateWithPartitioner,
                transactionRecord.txnSeqNo(),
                transactionRecord);
    }

    public void publishTransactionEventToSpecificPartition(
            @Nullable Integer messageKey,
            @Nonnull TransactionRecord transactionRecord,
            @Nonnull int partitionNumber
    ) throws ProduceException {
        CompletableFuture<SendResult<Integer, TransactionRecord>> sendResultCompletableFuture;
        try{
            log.info("Publishing Txn Seq No.-{} || TransactionRecord event {} || kafka topic {} || partition no. : {}....",
                    transactionRecord.txnSeqNo(),
                    transactionRecord,
                    kafkaTopicName,
                    partitionNumber
            );
            if(messageKey!=null)
                sendResultCompletableFuture = kafkaTemplateWithoutPartitoner.send(
                        kafkaTopicName,
                        partitionNumber,
                        messageKey, /// message key
                        transactionRecord /// event or message
                ).orTimeout(5, TimeUnit.SECONDS);
            else
                sendResultCompletableFuture = kafkaTemplateWithoutPartitoner.send(
                        kafkaTopicName,
                        partitionNumber,
                        transactionRecord /// event or message
                ).orTimeout(5, TimeUnit.SECONDS);
            while(!sendResultCompletableFuture.isDone()){
                sleep(1000);
            } /// waiting for the publishing to complete
        } catch (Exception e) {
            throw new ProduceException(e.getMessage(), transactionRecord);
        }
        log.info("Publishing Txn Seq No.-{} || partition no. : {}....", transactionRecord.txnSeqNo(), partitionNumber);
    }

    private void produceTransactionHappenEventMessageKeyDemo(
            KafkaTemplate<Integer, TransactionRecord> kafkaTemplate,
            @Nullable Integer messageKey,
            @Nonnull TransactionRecord message
    ) throws ProduceException {
        log.info("Publishing to Kafka Topic {}",kafkaTopicName);
        try{
            log.info("Publishing Txn Seq No.-{} || TransactionRecord event {} || kafka topic {}....",
                    message.txnSeqNo(),
                    message,
                    kafkaTopicName
            );
            CompletableFuture<SendResult<Integer, TransactionRecord>> sendResultCompletableFuture;
            if(messageKey!=null)
                sendResultCompletableFuture = kafkaTemplate.send(
                        kafkaTopicName,
                        messageKey, /// message key
                        message /// event or message
                ).orTimeout(5, TimeUnit.SECONDS);
            else
                sendResultCompletableFuture = kafkaTemplate.send(
                        kafkaTopicName,
                        message /// event or message
                ).orTimeout(5, TimeUnit.SECONDS);
            while(!sendResultCompletableFuture.isDone()){
                sleep(1000);
            } /// waiting for the publishing to complete
        } catch (Exception e) {
            throw new ProduceException(e.getMessage(), message);
        }
        log.info("Txn Seq No.-{} || Published to kafka topic {}..!!", message.txnSeqNo(), kafkaTopicName);
    }
}
