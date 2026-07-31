// NOTE: should I use instance-oriented methods?
package com.dyrmgraph.transform;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.trim;

import java.util.Map;
import java.time.LocalDate;

/**
 * Functions are package-private (not public).
 * TransformUtil
 */
public final class TransformUtil {

    private TransformUtil() {
    }

    private static Dataset<Row> transform(Dataset<Row> input) {
        // This is an example
        return input
                .filter(col("eventId").isNotNull())
                .withColumn("eventId", trim(col("eventId")));
    }

    /**
     * NOTE: return something? for the downstream tasks?
     * 
     */
    static Map<String, Object> run(Map<LocalDate, Map<String, Helpers.Paths>> paths) {
        // Need bulk read the same dates
        SparkSession spark = DyrmgraphConnection.getSparkSession();

        Map<String, Object> result;

        try {
            for (Map<String, Helpers.Paths> tables : paths.values()) {
                for (Helpers.Paths tablePaths : tables.values()) {

                    Dataset<Row> input = spark.read()
                            .option("header", "true")
                            .csv(tablePaths.input());

                    // dynamic parquet path needs table name, partition date, version
                    Dataset<Row> output = transform(input);

                    output.write()
                            .mode("overwrite")
                            .parquet(tablePaths.output());
                }
            }
            result = Map.of("status", "is_success");

        } catch (Exception exception) {
            result = Map.of("status", "is_failure", "reason", exception.getMessage());
        } finally {
            spark.stop();
        }

        return result;
    }
}
