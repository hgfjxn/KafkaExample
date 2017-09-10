package win.hgfdodo.kafka.producer.async;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by guangfuhe on 2017/9/10.
 */
class CallBackAdapter implements Callback {
    private final static Logger log = LoggerFactory.getLogger(CallBackAdapter.class);

    public void onCompletion(RecordMetadata metadata, Exception exception) {
        if (exception == null) {
            log.info("complete produce message: " + metadata);
        } else {
            log.error("error while complete producing message", exception);
        }
    }
}
