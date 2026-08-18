/**
 * The Main class for the main transformation logic
 */
package com.dyrmgraph.transform;

import org.apache.spark.sql.SparkSession;

import com.dyrmgraph.transform.utils.DyrmgraphConnection;
import com.dyrmgraph.transform.utils.Helpers;
import com.dyrmgraph.transform.utils.QueryExecutor;
import com.dyrmgraph.transform.utils.Schema;
import static com.dyrmgraph.transform.utils.TransformUtil.*;
import com.dyrmgraph.transform.utils.TransformUtil.ValidationResult;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.LocalDate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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
            Map<LocalDate, Integer> result = QueryExecutor.getPendingJobs(conn);
            String bucket = Optional.ofNullable(System.getenv("BUCKET"))
                    .orElseThrow(() -> new IllegalStateException("Env var BUCKET is required."));
            Map<LocalDate, Map<String, Helpers.Paths>> paths = Helpers.buildPaths(result, bucket);
            Map<String, Object> transformResult = run(paths);

            String xcomPath = System.getenv("XCOM_PATH");
            Helpers.writeXCOM(xcomPath, transformResult);
        }
    }

    // Summary: for each date and for each table (gkg, events, mentions),
    // perform transformation without joins
    // this produces almost one on one tables (3 in total) and N exploded tables
    private static Map<String, Object> run(Map<LocalDate, Map<String, Helpers.Paths>> paths) {
        SparkSession spark = DyrmgraphConnection.getSparkSession();

        Map<String, Object> result = new HashMap<>();

        try {
            for (Map.Entry<LocalDate, Map<String, Helpers.Paths>> dateEntry : paths.entrySet()) {
                LocalDate date = dateEntry.getKey();
                for (Map.Entry<String, Helpers.Paths> tableEntry : dateEntry.getValue().entrySet()) {
                    String tableName = tableEntry.getKey();
                    Helpers.Paths tablePaths = tableEntry.getValue();
                    // 2. read csv using the key, e.g.,
                    // s3://bucket/bronze/gkg/date=2026-07-20/001500.csv
                    Dataset<Row> input = spark.read()
                            .schema(Schema.schemaMap.get(tableName)) // read the whole things as string
                            .option("delimeter", "\t")
                            .option("header", "false")
                            .csv(tablePaths.inputPath());

                    // 2.5 register UDFs if needed

                    // 3. validate schema (don't use beans)
                    ValidationResult validationResult = validateSchema(input, tableName, date);

                    // 3.5 invalids are partitioned by publication date for easy perusing
                    Map<String, Object> invalidStat = flushInvalidRows(
                            validationResult.invalid(),
                            tableName,
                            String.format(tablePaths.errorPath(), tableName));
                    result.put("validation_result", invalidStat); // to xcom

                    // 4. normalize tables and explode nested cols if necessary
                    Map<String, Dataset<Row>> dfs = normalizeTables(validationResult.valid(), tableName);

                    // 5. save to parquet to each partition
                    List<String> successList = new ArrayList<>();
                    for (Map.Entry<String, Dataset<Row>> dfEntry : dfs.entrySet()) {
                        String outputPath = String.format(tablePaths.outputPath(), dfEntry.getKey());
                        dfEntry.getValue().write() // bad casts will fail here
                                .mode("overwrite")
                                .parquet(outputPath);

                        successList.add(outputPath);
                    }
                    result.put("result_tables", successList);
                    // TODO: currently if one table validation fails, all the dt fails
                    // only the three tables should be atomic
                    // do this with async mode? atomic failure?

                }
            }
            // 6. return result
            result.put("status", "is_success");

        } catch (Exception exception) {
            result.put("status", "is_failure");
            result.put("reason", exception.getMessage());
        } finally {
            spark.stop();
        }

        return result;
    }

}