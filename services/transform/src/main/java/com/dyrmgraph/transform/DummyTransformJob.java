package com.dyrmgraph.transform;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.trim;

public final class DummyTransformJob {

    private DummyTransformJob() {
    }

    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println(
                    "Usage: DummyTransformJob <input-path> <output-path>");
            System.exit(2);
        }

        String inputPath = args[0];
        String outputPath = args[1];

        SparkSession spark = SparkSession.builder()
                .appName("normalize-events")
                .getOrCreate();

        try {
            Dataset<Row> input = spark.read()
                    .parquet(inputPath);

            Dataset<Row> output = transform(input);

            output.write()
                    .mode("overwrite")
                    .parquet(outputPath);
        } finally {
            spark.stop();
        }
    }

    static Dataset<Row> transform(Dataset<Row> input) {
        return input
                .filter(col("eventId").isNotNull())
                .withColumn("eventId", trim(col("eventId")));
    }
}