package win.hgfdodo.kafka.producer.sync;


import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import win.hgfdodo.kafka.Person;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Created by guangfuhe on 2017/9/7.
 */
public class SyncProducer {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties prop = new Properties();
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        prop.put("schema.registry.url", "http://localhost:8081");

        KafkaProducer producer = new KafkaProducer(prop);

        long start = System.nanoTime();
        for (int i = 0; i < 100; i++) {
            ProducerRecord<String, Person> record = new ProducerRecord<String, Person>("start", String.valueOf(i), new Person(i, "hgf" + i));
            System.out.println(producer.send(record).get());
        }
        System.out.println("waste:" + (System.nanoTime() - start));
        producer.close();
    }

}
