package win.hgfdodo.kafka.consumer.manual.commit;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import win.hgfdodo.kafka.Person;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by guangfuhe on 2017/9/10.
 */
public class ManualCommitOffset {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put("schema.registry.url", "http://localhost:8081");

        final KafkaConsumer<String, Person> kafkaConsumer = new KafkaConsumer<String, Person>(properties);

        final Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            public void run() {
                System.out.println("starting exit...");
                try {
                    mainThread.join(10000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                kafkaConsumer.wakeup();
            }
        }));

        List<String> topics = new ArrayList<String>();
        topics.add("start");
        kafkaConsumer.subscribe(topics);

        try {
            while (true) {
                ConsumerRecords<String, Person> records = kafkaConsumer.poll(100);
                for (ConsumerRecord<String, Person> record : records) {
                    System.out.println(System.currentTimeMillis() + ":"
                            + record.topic() + ","
                            + record.partition() + ", "
                            + record.key() + ", "
                            + record.value() + ", "
                            + record.offset() + ", "
                            + record.headers());
                    kafkaConsumer.commitAsync(new OffsetCommitCallbackAdapter());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            kafkaConsumer.commitSync();
            kafkaConsumer.close();
            System.out.println("kafka consumer closed...");
        }

    }
}

