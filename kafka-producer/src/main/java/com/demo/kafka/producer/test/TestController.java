package com.demo.kafka.producer.test;

import com.demo.kafka.model.TransactionRecord;
import com.demo.kafka.producer.enums.DemoType;
import com.demo.kafka.producer.service.TransactionProducer;
import com.demo.kafka.producer.enums.PaymentMode;
import com.demo.kafka.producer.enums.TransactionType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Random;

@RestController
@RequestMapping("/kafka/demo/produce")
public class TestController {
    private static int txnSeqNo=0;

    @Autowired
    private TransactionProducer transactionProducer;

    @PostMapping("without-key")
    public ResponseEntity<String> produceEventsWithoutMessageKeyOrNullKey(@RequestParam("n") int n){
        /// calls the service code to publish messages without a message key or pass key as null
        try{
            publishSomeDummyEvents(n, null, DemoType.WITHOUT_MESSAGE_KEY);
        }catch (Exception e){
            return ResponseEntity.internalServerError().body(e.getMessage());
        }
        return ResponseEntity.ok("Published");
    }

    @PostMapping("with-key")
    public ResponseEntity<String> produceEventsWithMessageKey(@RequestParam("n") int n){
        /// calls the service code to publish messages with a message key.
        try{
            publishSomeDummyEvents(n, null,DemoType.WITH_MESSAGE_KEY);
        }catch (Exception e){
            return ResponseEntity.internalServerError().body(e.getMessage());
        }
        return ResponseEntity.ok("Published");
    }

    @PostMapping("to-specific-partition")
    public ResponseEntity<String> publishMessagesToSpecificPartitionOnly(
            @RequestParam("partition-number") int partitionNumber,
            @RequestParam("n") int n){
        /// calls the service code to publish messages with/without key
        /// to the partition number mentioned in the request parameter
        try{
            publishSomeDummyEvents(n, partitionNumber, DemoType.TO_SPECIFIC_PARTITION);
        }catch (Exception e){
            return ResponseEntity.internalServerError().body(e.getMessage());
        }
        return ResponseEntity.ok("Published to partition " + partitionNumber);
    }

    @PostMapping("using-custom-partition")
    public ResponseEntity<String> publishMessagesWithCustomPartitionSelector(@RequestParam("n") int n){
        /// calls the service code to publish messages
        /// using a custom partitioner, that contains the logic
        /// to select the partition of the topic where the message has to be published
        try{
            publishSomeDummyEvents(n, null, DemoType.WITH_CUSTOM_PARTITIONER);
        }catch (Exception e){
            return ResponseEntity.internalServerError().body(e.getMessage());
        }
        return ResponseEntity.ok("Published using custom partitioner");
    }

    public ResponseEntity<String> publishSomeDummyEvents(int numOfEventsToPublish, Integer partitionNumber, DemoType demoType){
        Random random = new Random();
        try{
            while(numOfEventsToPublish>0) {
                TransactionRecord transactionRecord = new TransactionRecord(
                        TransactionType.values()[random.nextInt(0,263)%TransactionType.values().length],
                        PaymentMode.values()[random.nextInt(0,350)%PaymentMode.values().length],
                        LocalDateTime.now(ZoneId.of("Asia/Kolkata")).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME),
                        new Random().nextFloat(100.78f, 38993.48f),
                        txnSeqNo,
                        String.format("Txn - %d", txnSeqNo)
                );
                switch (demoType){
                    case WITHOUT_MESSAGE_KEY -> transactionProducer.publishTransactionEventWithoutKey(transactionRecord);
                    case WITH_MESSAGE_KEY -> transactionProducer.publishTransactionEventWithKey(transactionRecord);
                    case WITH_CUSTOM_PARTITIONER -> transactionProducer.publishTransactionEventCustomPartition(transactionRecord);
                    case TO_SPECIFIC_PARTITION -> {
                        transactionProducer.publishTransactionEventToSpecificPartition(
                                transactionRecord.txnSeqNo(),
                                transactionRecord,
                                partitionNumber
                        );
                    }
                }
                txnSeqNo += 1;
                numOfEventsToPublish--;
            }
            return ResponseEntity.ok("Published");
        }catch (Exception e){
            return ResponseEntity.internalServerError().body(e.getMessage());
        }
    }
}
