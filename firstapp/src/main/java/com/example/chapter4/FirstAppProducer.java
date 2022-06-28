package com.example.chapter4;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

/**
 * Created by leaf on 2022/06/28
 */
public class FirstAppProducer {
    private static String topicName = "first-app";

    public static void main(String[] args) {
        Properties conf = new Properties();
        conf.setProperty("bootstrap.servers", "192.168.21.173:9092");
        conf.setProperty("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        conf.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<Integer, String> producer = new KafkaProducer<>(conf);

        int key;
        String value;
        for (int i = 1; i <= 10; i++) {
            key =i ;
            value = String.valueOf(i);

            ProducerRecord<Integer, String> record = new ProducerRecord<>(topicName, key, value);

            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (recordMetadata != null) {
                        String infoString = String.format("Success partition:%d, offset:%d", recordMetadata.partition(), recordMetadata.offset());
                        System.out.println(infoString);
                    } else {
                        String infoString = String.format("Failed:%s", e.getMessage());
                        System.err.println(infoString);
                    }
                }
            });
        }
        producer.close();

    }
}
