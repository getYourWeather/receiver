package com.weather.receiver.kafka.config;

import java.util.HashMap;
import java.util.Map;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

@Configuration
@EnableKafka
public class KafkaConfig {

    @Value("${kafka.servers:localhost:9092}")
    private String kafkaServers;

    @Bean
    public ProducerFactory<String, GenericRecord> producerFactory() {
        return new DefaultKafkaProducerFactory<>(producerConfigs());
    }

    @Bean
    public Map<String, Object> producerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServers);
        props.put(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "com.weather.info.avro.AvroSerializer");
        return props;
    }

    @Bean
    public KafkaTemplate<String, GenericRecord> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }
}
