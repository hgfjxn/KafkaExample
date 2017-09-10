package win.hgfdodo.kafka.producer.async;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import win.hgfdodo.kafka.Person;

import java.util.Properties;

/**
 * Created by guangfuhe on 2017/9/10.
 */
public class AsyncProducer {
    public static void main(String[] args) throws InterruptedException {
        Properties prop = new Properties();
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        prop.put("schema.registry.url", "http://localhost:8081");

        KafkaProducer producer = new KafkaProducer(prop);
        long start = System.nanoTime();
        for (int i = 0; i < 100; i++) {
            ProducerRecord<String, Person> record = new ProducerRecord<String, Person>("start", String.valueOf(i), new Person(i, "hgf" + i));
            System.out.println(producer.send(record, new CallBackAdapter()));
        }

        System.out.println("waste:" + (System.nanoTime() - start));
        producer.close();
    }
}

