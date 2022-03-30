package com.pixar.kafka.Consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * @author 姚
 */
public class KafkaConsumer {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers","127.0.0.1:9092");
        properties.put("key.deserializer", StringDeserializer.class);
        properties.put("value.deserializer",StringDeserializer.class);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test1");


        org.apache.kafka.clients.consumer.KafkaConsumer<String, String> consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(properties);

        consumer.subscribe(Collections.singleton("topic-1"));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));

            for (ConsumerRecord<String, String> record : records){
                System.out.println(String.format("topic:%s 分区, %d 偏移量 , %d," + "key:%s ,value:%s"
                        , record.topic(), record.offset(), record.key(), record.value()));
            }
        }

    }



}
