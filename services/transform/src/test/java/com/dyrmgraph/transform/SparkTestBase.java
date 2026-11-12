package com.dyrmgraph.transform;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Encoders;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.AfterAll;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.io.IOException;
import java.net.URISyntaxException;

import com.dyrmgraph.transform.utils.Schema;

public abstract class SparkTestBase {
    protected static SparkSession spark;
    protected static Map<String, Dataset<Row>> mockTables = new HashMap<>();
    protected static Map<String, Map<String, Dataset<Row>>> correctTables = Map.of(
            "gkg", new HashMap<>(),
            "events", new HashMap<>(),
            "mentions", new HashMap<>());

    @BeforeAll
    static void init() throws IOException, URISyntaxException {
        getSpark();
        initMockDFs();
        initCorrectDFs();
        // tempGenerateCorrectJson();
    }

    static void getSpark() {
        spark = SparkSession.builder()
                .master("local[2]")
                .appName("SparkTests")
                .config("spark.ui.enabled", "false")
                .config("spark.log.level", "ERROR")
                .getOrCreate();
    }

    static void initMockDFs() throws IOException, URISyntaxException {
        File file = new File(
                SparkTestBase.class.getClassLoader().getResource("testData.json").toURI());

        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = mapper.readTree(file);

        root.properties().forEach(entry -> {
            List<String> rows = new ArrayList<>();
            entry.getValue().forEach(row -> rows.add(row.toString()));
            // NOTE: json() turns non nulls to nullables
            Dataset<Row> df = spark.read()
                    .schema(Schema.schemaMap.get(entry.getKey()))
                    .json(
                            spark.createDataset(rows, Encoders.STRING()));
            mockTables.put(entry.getKey(), df);
        });
    }

    static void initCorrectDFs() throws IOException, URISyntaxException {
        for (Map.Entry<String, Map<String, Dataset<Row>>> tableEntry : correctTables.entrySet()) {
            File[] files = new File(
                    SparkTestBase.class.getClassLoader().getResource(tableEntry.getKey()).toURI()).listFiles();

            for (File file : files) {
                String resultTableName = file.getName().split("\\.")[0];
                Dataset<Row> df = spark.read()
                        .schema(Schema.schemaMap.get(resultTableName))
                        .json(file.getAbsolutePath());
                tableEntry.getValue().put(resultTableName, df);
            }
        }
    }

    /*
     * Temporary function to generate data.
     * Data is validated manually retroactively
     */
    // static void tempGenerateCorrectJson() throws IOException {
    // LocalDateTime dt = LocalDateTime.of(2026, 10, 22, 10, 30, 00);
    // for (Map.Entry<String, Dataset<Row>> tableEntry : mockTables.entrySet()) {
    // ValidationResult res = validateSchema(
    // tableEntry.getValue(),
    // tableEntry.getKey(),
    // dt);
    // Map<String, Dataset<Row>> dfs = normalizeTable(res.valid(),
    // tableEntry.getKey());

    // for (Map.Entry<String, Dataset<Row>> dfEntry : dfs.entrySet()) {
    // String outputPath = String.format("%s/%s/correctResult.json",
    // tableEntry.getKey(), dfEntry.getKey());
    // dfEntry.getValue().coalesce(1).write()
    // .mode("overwrite")
    // .json(outputPath);
    // }
    // }
    // }

    @AfterAll
    static void tearDown() {
        spark.stop();
    }

}
