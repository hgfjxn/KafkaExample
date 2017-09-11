package win.hgfdodo.kafka.consumer.rebalance;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

public class ConsumerRebalanceListenerDemo implements ConsumerRebalanceListener {
    private final static Logger log = LoggerFactory.getLogger(ConsumerRebalanceListener.class);

    //before revoke consumer's ownership of a partition.
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        log.info("before revoke partition ownership! will revoke={}", partitions);
    }

    //after partition asigned to a consumer
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        log.info("after asigned partitions! asigned={}" + partitions);
    }
}
