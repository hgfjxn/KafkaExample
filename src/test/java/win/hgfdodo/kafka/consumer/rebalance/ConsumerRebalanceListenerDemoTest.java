package win.hgfdodo.kafka.consumer.rebalance;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import win.hgfdodo.kafka.Person;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class ConsumerRebalanceListenerDemoTest {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
        properties.put("schema.registry.url", "http://localhost:8081");

        KafkaConsumer<String, Person> kafkaConsumer = new KafkaConsumer<String, Person>(properties);
        List<String> topics = new ArrayList<String>();
        topics.add("start");
        kafkaConsumer.subscribe(topics, new ConsumerRebalanceListenerDemo());
        while (true) {
            ConsumerRecords<String, Person> records = kafkaConsumer.poll(100);
            for (ConsumerRecord<String, Person> record : records) {
                System.out.println(System.currentTimeMillis() + ":" + record.topic() + ","
                        + record.partition() + ", "
                        + record.key() + ", "
                        + record.value() + ", "
                        + record.offset() + ", "
                        + record.headers());
            }
        }
    }

}