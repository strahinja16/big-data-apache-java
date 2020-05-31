import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

public class Main {
    private static String Topic = "weather-data";

    public static void main(String[] args) throws Exception {

        System.out.println("RUNNING STREAMING-CONSUMER");
        String initialSleepTime = System.getenv("INITIAL_SLEEP_TIME_IN_SECONDS");
        if (initialSleepTime != null && !initialSleepTime.equals("")) {
            int sleep = Integer.parseInt(initialSleepTime);
            System.out.println("Sleeping on start " + sleep + "sec");
            Thread.sleep(sleep * 1000);
        }
        String kafkaUrl = System.getenv("KAFKA_URL");
        if (kafkaUrl == null || kafkaUrl.equals("")) {
            throw new IllegalStateException("KAFKA_URL environment variable must be set");
        }

        String mongoUrl = System.getenv("MONGO_URL");
        if (mongoUrl == null || mongoUrl.equals("")) {
            throw new IllegalStateException("MONGO_URL environment variable must be set");
        }

        String dataReceivingTimeInSeconds = System.getenv("DATA_RECEIVING_TIME_IN_SECONDS");
        if (dataReceivingTimeInSeconds == null || dataReceivingTimeInSeconds.equals("")) {
            throw new IllegalStateException("DATA_RECEIVING_TIME_IN_SECONDS environment variable must be set");
        }
        int dataReceivingSleep = Integer.parseInt(dataReceivingTimeInSeconds);


        KafkaConsumer<String, String> consumer = configureConsumer(kafkaUrl);
        List<String> topics = new ArrayList<String>();
        topics.add(Topic);
        consumer.subscribe(topics);

        long pollTimeOut = 1000;
        long waitTime = dataReceivingSleep * 1000;

        while (true) {
            Thread.sleep(waitTime);

            // Request unread messages from the topic.
            ConsumerRecords<String, String> msg = consumer.poll(pollTimeOut);
            if (msg.count() != 0) {
                Iterator<ConsumerRecord<String, String>> iter = msg.iterator();
                while (iter.hasNext()) {
                    ConsumerRecord<String, String> record = iter.next();
                    System.out.println(record.value());

                    EventData data = EventData.CreateEventData(record.value());
                    if (data == null) {
                        continue;
                    }

                    System.out.println("[KAFKA DATA RECEIVED}]: " + data.getEventId());

                    MongoClientURI connectionString = new MongoClientURI(mongoUrl);
                    MongoClient mongoClient = new MongoClient(connectionString);
                    MongoDatabase database = mongoClient.getDatabase("weather");
                    MongoCollection<Document> collection = database.getCollection("stream-data");
                    Document doc = new Document("eventId", data.getEventId())
                            .append("startTime",data.getStartTime())
                            .append("endTime", data.getEndTime())
                            .append("country",data.getCountry());
                    collection.insertOne(doc);
                    mongoClient.close();
                }
            }
        }
    }

    public static KafkaConsumer<String, String> configureConsumer(String brokers) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "streaming-consumer");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "streaming-consumer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_DOC, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        return new KafkaConsumer<String, String>(props);
    }
}
