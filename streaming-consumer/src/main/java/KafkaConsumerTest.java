import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

public class KafkaConsumerTest {
    private static String Topic = "traffic-data";

    public static void kafkaProducerTest(String[] args) throws Exception {

        System.out.println("RUNNING STREAMING-CONSUMER");

        Thread.sleep(2000);

        String kafkaUrl = System.getenv("KAFKA_URL");
        if (kafkaUrl == null || kafkaUrl.equals("")) {
            throw new IllegalStateException("KAFKA_URL environment variable must be set");
        }

        KafkaConsumer<String, Integer> consumer = configureConsumer(kafkaUrl);
        List<String> topics = new ArrayList<String>();
        topics.add(Topic);
        consumer.subscribe(topics);

        long pollTimeOut = 1000;
        long waitTime = 5 * 1000;

        long numberOfMsgsReceived = 0;

        while (true) {
            // Request unread messages from the topic.
            ConsumerRecords<String, Integer> msg = consumer.poll(pollTimeOut);

            if (msg.count() != 0) {
                numberOfMsgsReceived += ((ConsumerRecords) msg).count();

                Iterator<ConsumerRecord<String, Integer>> iter = msg.iterator();
                while (iter.hasNext()) {
                    ConsumerRecord<String, Integer> record = iter.next();
                    System.out.println(record.value());
                }
            }
        }
    }

    public static KafkaConsumer<String, Integer> configureConsumer(String brokers) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "streaming-consumer");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "streaming-consumer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_DOC, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());

        return new KafkaConsumer<String, Integer>(props);
    }
}
