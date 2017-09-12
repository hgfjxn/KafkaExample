package win.hgfdodo.kafka.consumer.deserializer;

import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import win.hgfdodo.kafka.Person;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Map;

public class PersonDeserializer implements Deserializer<Person> {
    private final static Logger log = LoggerFactory.getLogger(PersonDeserializer.class);

    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    public Person deserialize(String topic, byte[] data) {
        if(data==null || data.length == 0){
            log.warn("deserializer data length is 0 or data is null!");
            return null;
        }
        ByteBuffer buffer = ByteBuffer.wrap(data);
        int id = buffer.getInt();
        int namelength = buffer.getInt();
        if(namelength == 0){
            return new Person(id, null);
        }else{
            Charset charset = Charset.forName("UTF-8");
            byte[] nameBytes = new byte[data.length-8];
            buffer.get(nameBytes);
            String name = new String(nameBytes, charset);
            return new Person(id, name);
        }
    }

    public void close() {

    }
}
