package com.demo.kafka.producer.partitioners;

import com.demo.kafka.model.TransactionRecord;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

public class MyCustomPartition implements Partitioner {
    @Override
    public int partition(String topicName, Object msgKey, byte[] msgKeyBytes, Object msgValue, byte[] msgValueBytes, Cluster cluster) {
        TransactionRecord txnRec = (TransactionRecord) msgValue;
        return txnRec.txnSeqNo()%2;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
