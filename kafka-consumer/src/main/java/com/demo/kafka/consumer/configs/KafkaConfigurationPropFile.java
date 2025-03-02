package com.demo.kafka.consumer.configs;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

@Configuration
@Profile("propFile")
public class KafkaConfigurationPropFile
{

    @Value("${kafka.topic.prod}")
    private String kafKaTopicName;


    @Bean("kafkaTopicName")
    public String topicName() {
        return this.kafKaTopicName;
    }

}
