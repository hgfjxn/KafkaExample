package win.hgfdodo.kafka.consumer.manual.commit;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by guangfuhe on 2017/9/10.
 */
class OffsetCommitCallbackAdapter implements OffsetCommitCallback {
    private final static Logger log = LoggerFactory.getLogger(OffsetCommitCallbackAdapter.class);

    public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
        if(null == exception){
            log.info("offset commit complete!");
            for(TopicPartition topicPartition: offsets.keySet()){
                log.info(topicPartition.topic()+"-"+topicPartition.partition()+":"+offsets.get(topicPartition).offset());
            }
        }else{
            log.error("offset commit exception", exception);
        }
    }
}
