/**
 * The Main class for the main transformation logic
 */
package com.dyrmgraph.transform;

import org.apache.spark.sql.SparkSession;

import com.dyrmgraph.transform.exceptions.ThresholdExceededException;
import com.dyrmgraph.transform.utils.DyrmgraphConnection;
import com.dyrmgraph.transform.utils.Helpers;
import com.dyrmgraph.transform.utils.QueryExecutor;
import com.dyrmgraph.transform.utils.Schema;
import static com.dyrmgraph.transform.utils.TransformUtil.*;
import com.dyrmgraph.transform.utils.TransformUtil.ValidationResult;

import io.jsonwebtoken.lang.Arrays;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.LocalDateTime;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.HashSet;
import java.io.StringWriter;
import java.io.PrintWriter;

public final class GDELTTables {

    private GDELTTables() {
    }

    public static void main(String[] args) throws SQLException, IOException, IllegalStateException {

        // Probably not needed. Inputs are read from PG
        // if (args.length != 2) {
        // System.err.println(
        // "Usage: DummyTransformJob <input-path> <output-path>");
        // System.exit(2);
        // }

        try (Connection conn = DyrmgraphConnection.getPGConn()) {
            // 1. fetch a job marked "claimed" and update it as "running"
            List<LocalDateTime> result = QueryExecutor.getPendingJobs(conn);
            String bucket = Optional.ofNullable(System.getenv("BUCKET"))
                    .orElseThrow(() -> new IllegalStateException("Env var BUCKET is required."));
            String ingestionBucket = Optional.ofNullable(System.getenv("CSV_INGESTION_BUCKET"))
                    .orElseThrow(() -> new IllegalStateException("Env var CSV_INGESTION_BUCKET is required."));
            Map<LocalDateTime, Map<String, Helpers.Paths>> paths = Helpers.buildPaths(result, bucket, ingestionBucket);

            SparkSession spark = DyrmgraphConnection.getSparkSession();
            String thresholdStr = Optional.ofNullable(System.getenv("VALIDATION_THRESHOLD"))
                    .orElseThrow(() -> new IllegalStateException("Env var VALIDATION_THRESHOLD is required"));
            double thresholdDbl = Double.parseDouble(thresholdStr);
            Map<String, Object> tfResult = run(spark, paths, thresholdDbl);
            // Map<String, Object> resultTwo = runGraphTF(resultOne);

            String xcomPath = System.getenv("XCOM_PATH");
            Helpers.writeXCOM(spark, xcomPath, tfResult);
        }
    }

    // NOTE: Not using a graphdb. I don't need a persistent graph.
    // instead, build a graph using spark-native graph compute at query time
    // private static Map<String, Object> runGraphTF(Map<String, Object> tfResult) {
    // SparkSession spark = DyrmgraphConnection.getSparkSession();
    // // result contains these keys: status, run_date, result_tables,
    // // validation_result, reason(depends on status)
    // try {
    // if (tfResult.get("status") == "is_success") {
    // // construct object path using date and output path inside result_tables
    // // and then read it. table name = silver_event_network
    // if (tfResult.get("result_tables") instanceof List<?>) {
    // List<?> objectList = (List<?>) tfResult.get("result_tables");
    // for (Object objectPath : objectList) {
    // if (objectPath instanceof String) {
    // // TODO: this doesn't need to read date if it is full refresh
    // // If it does, then i need to concat batch_time also
    // StringBuilder sb = new StringBuilder();
    // sb.append((String) objectPath);
    // sb.append(tfResult.get("run_date"));
    // String yap = sb.toString();
    // Dataset<Row> input = spark.read().parquet((String) objectPath);
    // // still a spark df
    // Dataset<Row> result = parquetToGraph(input);
    // } else {
    // throw new IllegalArgumentException("objectPath must be a String");
    // }
    // }
    // } else {
    // throw new IllegalArgumentException("result_tables must be a List");
    // }
    // // transform it into graph
    // // load it on to neo4j
    // }
    // } catch (Exception exception) {
    // tfResult.put("status", "is_failure");
    // tfResult.put("reason", exception.getMessage());
    // } finally {
    // spark.stop();
    // }
    // return tfResult;
    // }

    // Summary: for each date and for each table (gkg, events, mentions),
    // perform transformation without joins
    // this produces almost one on one tables (3 in total) and N exploded tables
    private static Map<String, Object> run(SparkSession spark, Map<LocalDateTime, Map<String, Helpers.Paths>> paths,
            Double threshold) {
        // xcom payload
        Map<String, Object> result = new HashMap<>();

        // This statistics can expand later
        Map<String, Number> totalStat = new HashMap<>(Map.of(
                "invalid_total", (long) 0,
                "valid_total", (long) 0));

        try {
            for (Map.Entry<LocalDateTime, Map<String, Helpers.Paths>> dateEntry : paths.entrySet()) {
                Map<String, Object> thisDate = new HashMap<>();
                LocalDateTime dt = dateEntry.getKey();
                thisDate.put("run_date", dt.toString());

                Map<String, Object> invalidResultsByTable = new HashMap<>();
                Map<String, List<String>> successResultsByTable = new HashMap<>();
                for (Map.Entry<String, Helpers.Paths> tableEntry : dateEntry.getValue().entrySet()) {
                    String tableName = tableEntry.getKey();

                    Helpers.Paths tablePaths = tableEntry.getValue();
                    // 2. read csv using the key, e.g.,
                    // s3://bucket/bronze/gkg/date=2026-07-20/001500.csv
                    Dataset<Row> input = spark.read()
                            .schema(Schema.schemaMap.get(tableName)) // read the whole things as string
                            .option("delimiter", "\t")
                            .option("header", "false")
                            .csv(tablePaths.inputPath());

                    // 3. validate schema (don't use beans)
                    ValidationResult validationResult = validateSchema(input, tableName, dt);

                    // 3.5 invalids are partitioned by publication date for easy perusing
                    Map<String, Object> invalidStat = flushInvalidRows(
                            validationResult.invalid(),
                            tableName,
                            String.format(tablePaths.errorPath(), tableName));

                    invalidResultsByTable.put(tableName, invalidStat);

                    if (invalidStat.get("invalid_row_count") instanceof Long cnt) {
                        totalStat.merge("invalid_total", cnt, (a, b) -> (long) a + (long) b);
                    }

                    // 4. normalize tables and explode nested cols if necessary
                    Map<String, Dataset<Row>> dfs = normalizeTable(validationResult.valid(), tableName);

                    totalStat.merge("valid_total", validationResult.valid().count(), (a, b) -> (long) a + (long) b);

                    // 5. save to parquet to each partition
                    // TODO: loose file_datetime since it is no longer used and does not add value
                    List<String> successList = new ArrayList<>();
                    for (Map.Entry<String, Dataset<Row>> dfEntry : dfs.entrySet()) {
                        // output path has a deferred fomat string for the table name
                        String outputPath = String.format(tablePaths.outputPath(), dfEntry.getKey());
                        Set<String> columns = new HashSet<>(Arrays.asList(dfEntry.getValue().columns()));
                        // Dim tables use hash for partitioning, not date
                        if (columns.contains("partition_date")) {
                            dfEntry.getValue().write() // bad casts will fail here
                                    .mode("overwrite")
                                    .partitionBy("partition_date", "batch_time")
                                    .parquet(outputPath);
                        } else {
                            // NOTE: full refresh for lookup tables since it has relatively small
                            // cardinality.
                            Dataset<Row> incoming = dfEntry.getValue();

                            Path path = new Path(outputPath);
                            FileSystem fs = path.getFileSystem(
                                    spark.sparkContext().hadoopConfiguration());

                            if (!fs.exists(path)) {
                                // init
                                incoming.write()
                                        .mode("overwrite")
                                        .partitionBy("partition_hash")
                                        .parquet(outputPath);
                            } else {
                                // ... this is pain. just use Iceberg??! 😭
                                Dataset<Row> existing = spark.read().parquet(outputPath);
                                Dataset<Row> merged = mergeLookupTable(incoming, existing);

                                // String stagingPath = outputPath + "_staging";

                                merged.write() // bad casts will fail here
                                        .mode("overwrite")
                                        .partitionBy("partition_hash")
                                        // .parquet(stagingPath);
                                        .parquet(outputPath);
                            }
                        }

                        successList.add(outputPath);
                    }
                    successResultsByTable.put(tableName, successList);
                }
                thisDate.put("result_tables", successResultsByTable);
                thisDate.put("validation_result", invalidResultsByTable);
            }

            double validRatio = totalStat.get("valid_total").doubleValue()
                    / (totalStat.get("valid_total").longValue() + totalStat.get("invalid_total").longValue());
            boolean isSuccess = validRatio >= threshold;

            if (!isSuccess) {
                throw new ThresholdExceededException(
                        String.format(
                                "Data quality threshold failed: %.2f%% valid (minimum %.2f%%)",
                                validRatio * 100, threshold * 100));
            }

            result.put("status", "is_success");
            result.put("statistics", totalStat);

        } catch (Exception exception) {
            result.put("status", "is_failed");
            StringWriter sw = new StringWriter();
            exception.printStackTrace(new PrintWriter(sw));
            result.put("reason", sw.toString());
        } finally {
            spark.stop();
        }

        return result;
    }

}