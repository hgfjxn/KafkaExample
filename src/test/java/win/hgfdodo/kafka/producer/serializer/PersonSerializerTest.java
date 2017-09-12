package win.hgfdodo.kafka.producer.serializer;

import win.hgfdodo.kafka.Person;
import win.hgfdodo.kafka.consumer.deserializer.*;

public class PersonSerializerTest {
    public static void main(String[] args) {
        PersonSerializer personSerializer = new PersonSerializer();
        PersonDeserializer personDeserializer = new PersonDeserializer();

        Person p = new Person(1, "hgf");
        byte[] res = personSerializer.serialize("x", p);
        System.out.println("serialized to "+ res);
        Person pp = personDeserializer.deserialize("x", res);
        System.out.println("deserializer to "+ pp);

    }

}