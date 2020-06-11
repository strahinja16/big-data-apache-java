import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.classification.RandomForestClassificationModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.*;

import static org.apache.spark.sql.types.DataTypes.createStructField;

public class Main {
    private static final String Topic = "weather-data";

    public static void main(String[] args) throws Exception {

        System.out.println("RUNNING ML-STREAMING");
        String initialSleepTime = System.getenv("INITIAL_SLEEP_TIME_IN_SECONDS");
        if (initialSleepTime != null && !initialSleepTime.equals("")) {
            int sleep = Integer.parseInt(initialSleepTime);
            System.out.println("Sleeping on start " + sleep + "sec");
            Thread.sleep(sleep * 1000);
        }

        String hdfsUrl = System.getenv("HDFS_URL");
        if (hdfsUrl == null || hdfsUrl.equals("")) {
            throw new IllegalStateException("HDFS_URL environment variable must be set");
        }

        String sparkMasterUrl = System.getenv("SPARK_MASTER_URL");
        if (sparkMasterUrl == null || sparkMasterUrl.equals("")) {
            throw new IllegalStateException("SPARK_MASTER_URL environment variable must be set.");
        }
        String kafkaUrl = System.getenv("KAFKA_URL");
        if (kafkaUrl == null || kafkaUrl.equals("")) {
            throw new IllegalStateException("KAFKA_URL environment variable must be set");
        }
        String dataReceivingTimeInSeconds = System.getenv("DATA_RECEIVING_TIME_IN_SECONDS");
        if (dataReceivingTimeInSeconds == null || dataReceivingTimeInSeconds.equals("")) {
            throw new IllegalStateException("DATA_RECEIVING_TIME_IN_SECONDS environment variable must be set");
        }
        int dataReceivingSleep = Integer.parseInt(dataReceivingTimeInSeconds);
        System.out.println("INIT COMPLETED");

        Logger rootLogger = Logger.getRootLogger();
        rootLogger.setLevel(Level.ERROR);

        Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
        Logger.getLogger("org.spark-project").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder().appName("BigData-4-ML-Streaming").master(sparkMasterUrl).getOrCreate();

        System.out.println("SPARK STARTED" + spark.toString());
        System.out.println("\n\n");

        JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());
        JavaStreamingContext streamingContext = new JavaStreamingContext(javaSparkContext, new Duration(dataReceivingSleep * 1000));

        RandomForestClassificationModel model = RandomForestClassificationModel.load(hdfsUrl + "/big-data-weather/ml-model/");

        Thread.sleep(10);

        Map<String, Object> kafkaParams = getKafkaParams(kafkaUrl);
        Collection<String> topics = Collections.singletonList(Topic);

        JavaInputDStream<ConsumerRecord<Object, String>> stream =
                KafkaUtils.createDirectStream(
                        streamingContext,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.Subscribe(topics, kafkaParams)
                );

        JavaDStream<String> receivedData = stream.map(ConsumerRecord::value);
        JavaDStream<EventData> eventData = receivedData.map(EventData::CreateEventData);

        JavaDStream<Row> rows = eventData.map(c -> RowFactory.create(
                calculateSeason(c.getStartTime()),
                convertWeatherEventToNumber(c.getType()),
                Double.parseDouble(c.getLocationLat()),
                Double.parseDouble(c.getLocationLng())));

        rows.foreachRDD(d -> {
            StructType rowSchema = DataTypes.createStructType(
                    new StructField[]{
                            createStructField("Season", DataTypes.IntegerType, false),
                            createStructField("WeatherType", DataTypes.IntegerType, false),
                            createStructField("Lat", DataTypes.DoubleType, false),
                            createStructField("Lng", DataTypes.DoubleType, false),
                    });

            Dataset<Row> data = spark.createDataFrame(d, rowSchema);

            VectorAssembler vectorAssembler = new VectorAssembler()
                    .setInputCols(new String[]{"Season", "WeatherType", "Lat", "Lng"})
                    .setOutputCol("Features");

            Dataset<Row> transformed = vectorAssembler.transform(data);
            Dataset<Row> predictions = model.transform(transformed);
            predictions.show(100);
        });

        streamingContext.start();
        streamingContext.awaitTermination();
    }

    public static Map<String, Object> getKafkaParams(String brokers) {
        Map<String, Object> kafkaParams = new HashMap<>();
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

    public static int convertWeatherEventToNumber(String weatherType) {
        switch (weatherType) {
            case "Cold":
                return 0;
            case "Fog":
                return 1;
            case "Hail":
                return 2;
            case "Rain":
                return 3;
            case "Snow":
                return 4;
            case "Storm":
                return 5;
            default:
                return 6;
        }
    }

    private static Integer calculateSeason(String dateString) {
        int[] seasons =  new int [] { 4, 4, 4, 1, 1, 1, 2, 2, 2, 3, 3, 3 };

        DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
        DateTime startDate = formatter.parseDateTime(dateString);
        int month = startDate.getMonthOfYear();

        return seasons[month - 1];
    }
}
