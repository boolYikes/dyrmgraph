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

public abstract class SparkTestBase {
    protected static SparkSession spark;
    protected static Map<String, Dataset<Row>> mockTables = new HashMap<>();

    @BeforeAll
    static void init() throws IOException, URISyntaxException {
        getSpark();
        initMockDFs();
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
            Dataset<Row> df = spark.read().json(
                    spark.createDataset(rows, Encoders.STRING()));
            mockTables.put(entry.getKey(), df);
        });
    }

    @AfterAll
    static void tearDown() {
        spark.stop();
    }

}
