import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;

public class Main {
    public static void main(String[] args) {
        if (args.length < 2) {
            throw new IllegalArgumentException("Csv file paths on the hdfs must be passed as arguments");
        }

        String sparkMasterUrl = System.getenv("SPARK_MASTER_URL");
        if (sparkMasterUrl == null || sparkMasterUrl.equals("")) {
            throw new IllegalStateException("SPARK_MASTER_URL environment variable must be set.");
        }

        String hdfsUrl = System.getenv("HDFS_URL");
        if (hdfsUrl == null || hdfsUrl.equals("")) {
            throw new IllegalStateException("HDFS_URL environment variable must be set");
        }

        String weatherCsvFile = hdfsUrl + args[0];
        String trafficCsvFile = hdfsUrl + args[1];

        SparkSession spark = SparkSession.builder().appName("BigData-1").master(sparkMasterUrl).getOrCreate();

        Dataset<Row> weatherDataSet = spark.read().option("header", "true").csv(weatherCsvFile);
        Dataset<Row> trafficDataSet = spark.read().option("header", "true").csv(trafficCsvFile);

        GetCountriesSortedByMostSevereWinter(weatherDataSet, 2017);
        GetSeverityStatisticsPerEventTypeForCityInLastTenYears(weatherDataSet, "San Francisco");
        GetCityWithMinAndMaxBrokenVehiclesInFiveYears(trafficDataSet);

        spark.stop();
        spark.close();
    }

    static void GetCountriesSortedByMostSevereWinter(Dataset<Row> ds, int year)
    {
        String winterStart = String.format("%d-12-22 00:00:00", year);
        String winterEnd = String.format("%d-3-20 00:00:00", year + 1);

        ds.filter(
             ds.col("Severity").equalTo("Severe")
                 .and(ds.col("Type").equalTo("Cold"))
                 .and(ds.col("StartTime(UTC)").gt(winterStart)
                     .and(ds.col("EndTime(UTC)").lt(winterEnd)
                 )))
        .groupBy("State")
        .count()
        .orderBy(functions.col("count").desc())
        .show();
    }

    static void GetCityWithMinAndMaxBrokenVehiclesInFiveYears(Dataset<Row> ds) {
        String startDate = "2015-01-01 00:00:00";
        String endDate = "2020-01-01 00:00:00";

        ds.filter(
                ds.col("Type").equalTo("Broken-Vehicle")
                .and(ds.col("StartTime(UTC)").between(startDate, endDate))
                .and(ds.col("EndTime(UTC)").between(startDate, endDate))
        )
        .groupBy("City")
        .agg(functions.count("City"))
        .agg(functions.min("City"), functions.max("City"))
        .show();
    }

    static void GetSeverityStatisticsPerEventTypeForCityInLastTenYears(Dataset<Row> ds, String city)
    {
        String startDate = "2010-01-01 00:00:00";
        String endDate = "2020-01-01 00:00:00";

        UserDefinedFunction convertSeverityToToNumber = CreateUDFConvertSeverityToNumber();

        ds.filter(
            ds.col("Severity").isin("Light", "Moderate", "Heavy", "Severe")
                .and(ds.col("City").equalTo(city))
                .and(ds.col("StartTime(UTC)").between(startDate, endDate))
                .and(ds.col("EndTime(UTC)").between(startDate, endDate))
        )
        .groupBy("Type")
        .agg(
            functions.avg(convertSeverityToToNumber.apply(ds.col("Severity"))),
            functions.min(convertSeverityToToNumber.apply(ds.col("Severity"))),
            functions.max(convertSeverityToToNumber.apply(ds.col("Severity"))),
            functions.stddev(convertSeverityToToNumber.apply(ds.col("Severity")))
        ).show();
    }

    static UserDefinedFunction CreateUDFConvertSeverityToNumber()
    {
        UDF1<String, Integer> udfConvert = (severity) -> {
            switch (severity) {
                case "Light":
                    return 1;
                case "Moderate":
                    return 2;
                case "Heavy":
                    return 3;
                case "Severe":
                    return 4;
                default:
                    return 0;
            }
        };

        return functions.udf(udfConvert, DataTypes.IntegerType);
    }
}
