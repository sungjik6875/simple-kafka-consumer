import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;

public class SimpleConsumer {

    private static final Logger logger = LoggerFactory.getLogger(SimpleConsumer.class);

    private static final String TOPIC_NAME = "test";

    private static final int PARTITION_NUMBER = 0;

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    private static final String GROUP_ID = "test-group-2";

    public static void main(String[] args) {
        Properties configs = new Properties();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(configs);
        consumer.assign(Collections.singletonList(new TopicPartition(TOPIC_NAME, PARTITION_NUMBER)));

        Runtime.getRuntime().addShutdownHook(new ShutdownThread(consumer));

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));

                for (ConsumerRecord<String, String> record : records) {
                    logger.info("{}", record);
                }
            }
        } catch (WakeupException e) {
            logger.warn("Wakeup consumer");
            // 리소스 종료 처리
        } finally {
            consumer.close();
        }
    }

    private static class ShutdownThread extends Thread {
        private final KafkaConsumer<?, ?> consumer;

        public ShutdownThread(KafkaConsumer<?, ?> consumer) {
            this.consumer = consumer;
        }

        public void run() {
            logger.info("Shutdown hook");
            consumer.wakeup();
        }
    }

    private static class RebalanceListener implements ConsumerRebalanceListener {

        private final KafkaConsumer<?, ?> consumer;
        private final Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();

        public RebalanceListener(KafkaConsumer<?, ?> consumer) {
            this.consumer = consumer;
        }

        public void addOffset(TopicPartition topicPartition, OffsetAndMetadata offsetAndMetadata) {
            currentOffsets.put(topicPartition, offsetAndMetadata);
        }

        public void clearOffset() {
            currentOffsets.clear();
        }

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            logger.warn("Partitions are revoked");
            consumer.commitSync(currentOffsets);
            clearOffset();
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            logger.warn("Partitions are assigned");
        }
    }

}
