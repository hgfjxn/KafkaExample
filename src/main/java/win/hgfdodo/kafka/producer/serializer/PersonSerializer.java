package win.hgfdodo.kafka.producer.serializer;

import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import win.hgfdodo.kafka.Person;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Map;

public class PersonSerializer implements Serializer<Person> {
    private final static Logger log = LoggerFactory.getLogger(PersonSerializer.class);

    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    public byte[] serialize(String topic, Person data) {
        byte[] res = new byte[0];

        if (null != data) {
            int total = 4 + 4;
            int nameLength = 0;
            String name = data.getName().toString();
            if (name != null) {
                nameLength = name.length();
                total += nameLength;
            }
            res = new byte[total];
            ByteBuffer buffer = ByteBuffer.wrap(res);
            buffer.putInt(data.getId());
            buffer.putInt(nameLength);
            if (name != null) {
                buffer.put(name.getBytes(Charset.forName("UTF-8")));
            }
        }

        log.info("serialize data={} to buffer={}", data, res);
        return res;
    }

    public void close() {

    }
}
