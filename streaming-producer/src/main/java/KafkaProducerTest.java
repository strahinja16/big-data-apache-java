
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;

public class KafkaProducerTest {
    private final static String Topic = "traffic-data";

    public static void kafkaProducerTest(String[] args) throws Exception {

        System.out.println("RUNNING STREAMING-PRODUCER");
//        String initialSleepTime = System.getenv("INITIAL_SLEEP_TIME_IN_SECONDS");
//        if (initialSleepTime != null && !initialSleepTime.equals("")) {
//            int sleep = Integer.parseInt(initialSleepTime);
//            System.out.println("Sleeping on start " + sleep + "sec");
//            Thread.sleep(sleep * 1000);
//        }

        String hdfsUrl = System.getenv("HDFS_URL");
        if (hdfsUrl == null || hdfsUrl.equals("")) {
            throw new IllegalStateException("HDFS_URL environment variable must be set.");
        }
        String csvFilePath = System.getenv("CSV_FILE_PATH");
        if (csvFilePath == null || csvFilePath.equals("")) {
            throw new IllegalStateException("CSV_FILE_PATH environment variable must be set");
        }
        String kafkaUrl = System.getenv("KAFKA_URL");
        if (kafkaUrl == null || kafkaUrl.equals("")) {
            throw new IllegalStateException("KAFKA_URL environment variable must be set");
        }
        String dataSendingTimeInSeconds = System.getenv("DATA_SENDING_TIME_IN_SECONDS");
        if (dataSendingTimeInSeconds == null || dataSendingTimeInSeconds.equals("")) {
            throw new IllegalStateException("DATA_SENDING_TIME_IN_SECONDS environment variable must be set");
        }
        int dataSendingSleep = Integer.parseInt(dataSendingTimeInSeconds);

        KafkaProducer<String, Integer> producer = configureProducer(kafkaUrl);

        for (int i = 0; i < 10; i++)
        {
            ProducerRecord<String, Integer> rec = new ProducerRecord<String, Integer>(Topic, i);
            producer.send(rec);
            System.out.println("[KAFKA DATA SENT}]: " + i);
            Thread.sleep(dataSendingSleep * 1000);
            System.out.println("Sleeping " + dataSendingSleep + "sec");
        }

        producer.close();
        System.out.println("All done.");

        System.exit(0);
    }

    public static KafkaProducer<String, Integer> configureProducer(String brokers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "streaming-producer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        return new KafkaProducer<String, Integer>(props);
    }
}
