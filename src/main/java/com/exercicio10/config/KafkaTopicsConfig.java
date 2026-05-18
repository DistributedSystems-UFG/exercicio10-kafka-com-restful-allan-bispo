package com.exercicio10.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicsConfig {

    public static final String TOPIC_RAW = "temperature-raw";
    public static final String TOPIC_PROCESSED = "temperature-processed";

    @Bean
    public NewTopic topicRaw() {
        return TopicBuilder.name(TOPIC_RAW).partitions(1).replicas(1).build();
    }

    @Bean
    public NewTopic topicProcessed() {
        return TopicBuilder.name(TOPIC_PROCESSED).partitions(1).replicas(1).build();
    }
}
