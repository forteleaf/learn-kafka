package com.example.chapter4;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

/**
 * Created by leaf on 2022/06/29
 */
public class FirstAppConsumer {
    private static String topicName = "first-app";

    public static void main(String[] args) {

        Properties conf = new Properties();
        conf.setProperty("bootstrap.servers", "192.168.21.173:9092");
        conf.setProperty("group.id", "FirstAppConsumerGroup");
        conf.setProperty("enable.auto.commit", "false");
        conf.setProperty("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
        conf.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        Consumer<Integer, String> consumer = new KafkaConsumer<>(conf);

        consumer.subscribe(Collections.singletonList(topicName));

        for (int count = 0; count < 300; count++) {
            // kafka 하위 버전에서 Duration을 사용함
            ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofMinutes(1));
            for (ConsumerRecord<Integer, String> record : records) {
                String msgString = String.format("key:%d, value:%s", record.key(), record.value());
                System.out.println(msgString);

                // 처리가 완료 된 오프셋을 커밋
                TopicPartition tp = new TopicPartition(record.topic(), record.partition());
                OffsetAndMetadata                      oam        = new OffsetAndMetadata(record.offset() + 1);
                Map<TopicPartition, OffsetAndMetadata> commitInfo = Collections.singletonMap(tp, oam);
                consumer.commitSync(commitInfo);

            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

        }
        consumer.close();

    }
}
