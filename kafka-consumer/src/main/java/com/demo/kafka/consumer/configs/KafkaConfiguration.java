package com.demo.kafka.consumer.configs;

import com.demo.kafka.model.TransactionRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.support.serializer.JsonDeserializer;

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

    @Value("${kafka.consumer.deserializers.key}")
    private String keyDeserializerClassName;

    @Value("${kafka.consumer.deserializers.value}")
    private String valueDeserializerClassName;

    @Value("${kafka.consumer.offset_reset}")
    private String offsetResetStrategy;

    @Value("${kafka.message.trusted_package}")
    private String jsonDeserializerTrustedPackage;


    @Bean
    public String topicName() {
        return this.kafKaTopicName;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<Integer, TransactionRecord> concurrentKafkaListenerContainerFactory(
            ConsumerFactory<Integer, TransactionRecord> consumerFactory){
        ConcurrentKafkaListenerContainerFactory<Integer, TransactionRecord> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        return factory;
    }

    @Bean
    public ConsumerFactory<Integer, TransactionRecord> consumerFactory() {
        Map<String,Object> consumerProps = new HashMap<>();
        consumerProps.put(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                String.format("%s:%s",bootstrapServerHost,bootstrapServerPort)
        );
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializerClassName);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializerClassName);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetResetStrategy.toLowerCase());
        consumerProps.put(JsonDeserializer.TRUSTED_PACKAGES, jsonDeserializerTrustedPackage);

        return new DefaultKafkaConsumerFactory<>(consumerProps);
    }
}
