import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.ml.classification.RandomForestClassificationModel;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.IOException;

import static org.apache.spark.sql.types.DataTypes.*;

public class Main {
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            throw new IllegalArgumentException("Csv file path on the hdfs must be passed as argument");
        }

        String sparkMasterUrl = System.getenv("SPARK_MASTER_URL");
        if (sparkMasterUrl == null || sparkMasterUrl.equals("")) {
            throw new IllegalStateException("SPARK_MASTER_URL environment variable must be set.");
        }

        String hdfsUrl = System.getenv("HDFS_URL");
        if (hdfsUrl == null || hdfsUrl.equals("")) {
            throw new IllegalStateException("HDFS_URL environment variable must be set");
        }

        Logger rootLogger = Logger.getRootLogger();
        rootLogger.setLevel(Level.ERROR);

        Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
        Logger.getLogger("org.spark-project").setLevel(Level.WARN);

        String hdfsPath = args[0];
        String csvFile = hdfsUrl + hdfsPath;

        SparkSession spark = SparkSession.builder().appName("BigData-3").master(sparkMasterUrl).getOrCreate();
        Dataset<Row> dataSet = spark.read().option("header", "true").csv(csvFile);

        Dataset<Row> weather = dataSet
            .filter(
                dataSet.col("Type").equalTo("Cold")
                    .or(dataSet.col("Type").equalTo("Fog"))
                    .or(dataSet.col("Type").equalTo("Hail"))
                    .or(dataSet.col("Type").equalTo("Rain"))
                    .or(dataSet.col("Type").equalTo("Snow"))
                    .or(dataSet.col("Type").equalTo("Storm")))
            .select(
                dataSet.col("Type"),
                dataSet.col("Severity"),
                dataSet.col("StartTime(UTC)"),
                dataSet.col("LocationLat"),
                dataSet.col("LocationLng")
            );

        UDF1<String, Integer> udfCalculateSeason = Main::calculateSeason;
        UserDefinedFunction getSeason = functions.udf(udfCalculateSeason, DataTypes.IntegerType);

        UDF1<String, Integer> udfConvertWeatherType = Main::convertWeatherEventToNumber;
        UserDefinedFunction getWeatherType = functions.udf(udfConvertWeatherType, DataTypes.IntegerType);

        UDF1<String, Integer> udfConvertSeverity = Main::convertSeverityToNumber;
        UserDefinedFunction getSeverity = functions.udf(udfConvertSeverity, DataTypes.IntegerType);

        Dataset<Row> preparedData = weather.select(
            getSeverity.apply(weather.col("Severity")).as("WeatherSeverity"),
            getSeason.apply(weather.col("StartTime(UTC)")).as("Season"),
            getWeatherType.apply(weather.col("Type")).as("WeatherType"),
            weather.col("LocationLat").cast(DoubleType).as("Lat"),
            weather.col("LocationLng").cast(DoubleType).as("Lng")
        );

        VectorAssembler vectorAssembler = new VectorAssembler()
                .setInputCols(new String[]{"Season", "WeatherType", "Lat", "Lng"})
                .setOutputCol("Features");

        Dataset<Row> transformed = vectorAssembler.transform(preparedData);

        Dataset<Row>[] splits = transformed.randomSplit(new double[]{0.7, 0.3});
        Dataset<Row> trainingData = splits[0];
        Dataset<Row> testData = splits[1];

        RandomForestClassifier rf = new RandomForestClassifier()
                .setLabelCol("WeatherSeverity")
                .setFeaturesCol("Features");

        RandomForestClassificationModel model = rf.fit((trainingData));

        Dataset<Row> predictions = model.transform(testData);
        predictions.show(100);

        MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
                .setLabelCol("WeatherSeverity")
                .setPredictionCol("prediction")
                .setMetricName("accuracy");

        double accuracy = evaluator.evaluate(predictions);
        System.out.println("Accuracy = " + accuracy);
        System.out.println("Test Error = " + (1.0 - accuracy));

//        model.save(hdfsUrl + "/ml-model");

        spark.stop();
        spark.close();
    }

    public static int convertSeverityToNumber(String severity) {
        if (severity == null) {
            return 0;
        }
        switch (severity) {
            case "Light":
            case "Short":
            case "Fast":
                return 1;
            case "Moderate":
                return 2;
            case "Heavy":
                return 3;
            case "Severe":
            case "Slow":
            case "Long":
                return 4;
            default:
                return 0;
        }
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
