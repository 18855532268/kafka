package com.pixar.kafka.Producer;

import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @author 姚
 */
public class KafkaProducer {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers","127.0.0.1:9092");
        properties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");

        org.apache.kafka.clients.producer.KafkaProducer<String, String> producer = new org.apache.kafka.clients.producer.KafkaProducer<>(properties);

        ProducerRecord record ;
        for(int i = 0; i< 4; i++){
            record = new ProducerRecord<String, String>("topic-1", String.valueOf(i), "yao");
            producer.send(record);
            System.out.println(i+"message send...");
        }
    }
}
