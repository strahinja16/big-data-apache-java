import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.*;
import org.bson.Document;
import org.joda.time.DateTime;
import org.joda.time.Period;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import scala.Tuple2;
import java.util.*;
import scala.Tuple3;
import scala.Tuple4;
import scala.reflect.ClassTag;
import org.apache.spark.api.java.Optional;

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

        String sparkMasterUrl = System.getenv("SPARK_MASTER_URL");
        if (sparkMasterUrl == null || sparkMasterUrl.equals("")) {
            throw new IllegalStateException("SPARK_MASTER_URL environment variable must be set.");
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

        SparkConf conf = new SparkConf().setAppName("BigData-2").setMaster(sparkMasterUrl);
        JavaStreamingContext streamingContext = new JavaStreamingContext(conf, new Duration(dataReceivingSleep * 1000));
        streamingContext.checkpoint("./checkpoint");

        Map<String, Object> kafkaParams = getKafkaParams(kafkaUrl);
        Collection<String> topics = Collections.singletonList(Topic);

        JavaInputDStream<ConsumerRecord<Object, String>> stream =
                KafkaUtils.createDirectStream(
                        streamingContext,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.Subscribe(topics, kafkaParams)
                );

        String city = "Saguache";
        ClassTag<String> stringClassTag = scala.reflect.ClassTag$.MODULE$.apply(String.class);
        Broadcast<String> bcCity = streamingContext.ssc().sc().broadcast(city, stringClassTag);
        // Spark Streaming Processing
        JavaDStream<String> receivedData = stream.map(ConsumerRecord::value);
        JavaDStream<EventData> eventData = receivedData.map(EventData::CreateEventData);
        JavaDStream<EventData> rainData = eventData.filter(ed -> ed != null && ed.getType().equals("Rain") && bcCity.getValue().equals(ed.getCity()));
        JavaPairDStream<EventData, Integer> snowWithDuration = rainData.mapToPair(c -> new Tuple2<>(c, calculateDurationInMinutes(c)));

        JavaPairDStream<String, Integer> test = rainData.mapToPair(s -> new Tuple2<>(s.getCity(), calculateDurationInMinutes(s)));

        snowWithDuration.foreachRDD(d -> {
            d.foreach(c -> {
                MongoClientURI connectionString = new MongoClientURI(mongoUrl);
                MongoClient mongoClient = new MongoClient(connectionString);
                MongoDatabase database = mongoClient.getDatabase("weather");
                MongoCollection<Document> collection = database.getCollection("rain");
                Document doc = new Document("eventId", c._1.getEventId())
                        .append("type", c._1.getType())
                        .append("startTime", c._1.getStartTime())
                        .append("endTime", c._1.getEndTime())
                        .append("minutes", c._2)
                        .append("latitude", c._1.getLocationLat())
                        .append("longitude", c._1.getLocationLng())
                        .append("city", c._1.getCity());
                collection.insertOne(doc);
                mongoClient.close();
            });
        });

        Function3<String, Optional<Integer>, State<Tuple4<Integer, Integer, Double, Integer>>, Tuple3<Integer, Integer, Double>> mappingFunc =
                (word, val, state) -> {
                    Tuple4<Integer, Integer, Double, Integer> minMaxAvgCount = state.exists() ? state.get() : new Tuple4<>(Integer.MAX_VALUE, Integer.MIN_VALUE, 0.0, 0);

                    int min = minMaxAvgCount._1();
                    int max = minMaxAvgCount._2();
                    double avg = minMaxAvgCount._3();
                    int count = minMaxAvgCount._4();

                    if (val.get() > max) {
                        max = val.get();
                    }
                    if (val.get() < min) {
                        min = val.get();
                    }
                    avg = (avg * count + val.get()) / (count + 1);
                    count++;

                    Tuple4<Integer, Integer, Double, Integer> data = new Tuple4<>(min, max, avg, count);
                    Tuple3<Integer, Integer, Double> returnData = new Tuple3<>(min, max, avg);
                    state.update(data);

                    return returnData;
                };

        JavaMapWithStateDStream<String, Integer, Tuple4<Integer, Integer, Double, Integer>, Tuple3<Integer, Integer, Double>> stateDstream = test.mapWithState(StateSpec.function(mappingFunc));

        stateDstream.print();

        streamingContext.start();
        streamingContext.awaitTermination();
    }

    public static Map<String, Object> getKafkaParams(String brokers) {
        Map<String, Object> kafkaParams = new HashMap<String, Object>();
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        kafkaParams.put(ConsumerConfig.CLIENT_ID_CONFIG, "streaming-consumer");
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, "streaming-consumer");
        kafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_DOC, "earliest");
        kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        kafkaParams.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        return kafkaParams;
    }

    private static Integer calculateDurationInMinutes(EventData data) {
        DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");

        DateTime startDate = formatter.parseDateTime(data.getStartTime());
        DateTime endDate = formatter.parseDateTime(data.getEndTime());

        Period p = new Period(startDate, endDate);
        return p.getMinutes();
    }
}
