package com.demo.kafka.producer.configs;

import com.demo.kafka.model.TransactionRecord;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.RoundRobinPartitioner;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

@Configuration
@Profile("javaconf")
public class KafkaConfiguration
{
    @Value("${kafka.topic.default}")
    private String defaultKafKaTopicName;

    @Value("${kafka.topic.prod}")
    private String kafKaTopicName;

    @Value("${kafka.bootstrap_server.host}")
    private String bootstrapServerHost;

    @Value("${kafka.bootstrap_server.port}")
    private String bootstrapServerPort;

    @Value("${kafka.producer.serializers.key}")
    private String keySerializerClassName;

    @Value("${kafka.producer.serializers.value}")
    private String valueSerializerClassName;

    @Value("${kafka.producer.idempotent.enable}")
    private String safeProducerPropIdempotentEnable;

    @Value("${kafka.producer.idempotent.min_insync_replica}")
    private String safeProducerPropISR;

    @Value("${kafka.producer.idempotent.ack_type}")
    private String safeProducerPropAckMode;

    @Value("${kafka.producer.idempotent.retries}")
    private String safeProducerPropNumOfRetriesAllowed;

    @Value("${kafka.topic.partitioner.name}")
    private String customPartitioner;

    @Bean
    public String topicName() {
        return this.kafKaTopicName;
    }

    @Bean("kafkaTemplateWithoutPartitioner")
    public KafkaTemplate<Integer, TransactionRecord> kafkaTemplateWithoutPartitioner() {
        KafkaTemplate<Integer, TransactionRecord> kafkaTemplate = new KafkaTemplate<>(producerFactory(false));
        kafkaTemplate.setDefaultTopic(defaultKafKaTopicName);
        return kafkaTemplate;
    }

    @Bean("kafkaTemplateWithCustomPartitioner")
    public KafkaTemplate<Integer, TransactionRecord> kafkaTemplateWithCustomPartitioner() {
        KafkaTemplate<Integer, TransactionRecord> kafkaTemplate = new KafkaTemplate<>(producerFactory(true));
        kafkaTemplate.setDefaultTopic(defaultKafKaTopicName);
        return kafkaTemplate;
    }

    //@Bean
    public ProducerFactory<Integer, TransactionRecord> producerFactory(boolean customPartitionerRequired) {
        Map<String,Object> producerProps = new HashMap<>();
        producerProps.put(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                String.format("%s:%s",bootstrapServerHost,bootstrapServerPort)
        );
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializerClassName);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializerClassName);

        if(customPartitionerRequired && null!=customPartitioner && !customPartitioner.isEmpty())
            producerProps.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, customPartitioner);
        /*else
            producerProps.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, RoundRobinPartitioner.class);*/

        /// these properties of the producer helps to ensure
        /// that the events are successfully written on the Kafka topics
        /// without any loss
        if(Boolean.getBoolean(safeProducerPropIdempotentEnable.toLowerCase())){
            producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, safeProducerPropIdempotentEnable.toLowerCase());
            producerProps.put(ProducerConfig.ACKS_CONFIG, safeProducerPropAckMode.toLowerCase());
            producerProps.put(ProducerConfig.RETRIES_CONFIG, safeProducerPropNumOfRetriesAllowed);
            producerProps.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, safeProducerPropISR);
        }

        return new DefaultKafkaProducerFactory<>(producerProps);
    }
}
