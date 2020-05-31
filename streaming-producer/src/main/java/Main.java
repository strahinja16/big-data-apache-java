import java.io.*;
import java.net.URI;
import java.util.Properties;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Main {
    private final static String Topic = "weather-data";

    public static void main(String[] args) throws Exception {
        System.out.println("RUNNING STREAMING-PRODUCER");

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

        KafkaProducer<String, String> producer = configureProducer(kafkaUrl);

        Configuration conf = new Configuration();
        conf.set(CommonConfigurationKeys.FS_DEFAULT_NAME_KEY, hdfsUrl);
        conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", LocalFileSystem.class.getName());
        FSDataInputStream inputStream = null;
        FileSystem fs = null;

        try {
            fs = FileSystem.get(URI.create(hdfsUrl), conf);
            Path inFile = new Path(csvFilePath);
            inputStream = fs.open(inFile);

            if (!fs.exists(inFile)) {
                System.out.println("Input file not found");
                throw new IOException("Input file not found");
            }

            String line = inputStream.readLine();
            while (line != null) {
                EventData tmp = EventData.CreateEventData(line);
                if (tmp != null) {
                    ProducerRecord<String, String> rec = new ProducerRecord<String, String>(Topic, line);
                    producer.send(rec);
                    System.out.println("[KAFKA DATA SENT}]: " + tmp.getEventId());
                    Thread.sleep(dataSendingSleep * 1000);
                    System.out.println("Sleeping " + dataSendingSleep + "sec");
                }
                line = inputStream.readLine();
            }

        } finally {
            if (inputStream != null) {
                inputStream.close();
            }
            if (fs != null) {
                fs.close();
            }
        }

        producer.close();
        System.out.println("All done.");

        System.exit(0);
    }

    public static KafkaProducer<String, String> configureProducer(String brokers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "streaming-producer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<String, String>(props);
    }
}