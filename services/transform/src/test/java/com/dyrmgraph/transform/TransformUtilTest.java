/**
 * Component tests for actual transformation logic
 */
package com.dyrmgraph.transform;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.RowFactory;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.sort_array;
import static org.apache.spark.sql.functions.map_entries;

import org.opentest4j.AssertionFailedError;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dyrmgraph.transform.utils.TransformUtil.ValidationResult;
import com.dyrmgraph.transform.utils.Schema;
import static com.dyrmgraph.transform.utils.TransformUtil.*;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.Arrays;
import java.io.IOException;
import java.time.LocalDateTime;
import java.net.URISyntaxException;
import java.sql.Timestamp;

class TransformUtilTest extends SparkTestBase {

    @Test
    void testSparkSession() {
        assert spark != null;
        assert !spark.sparkContext().isStopped();
    }

    /**
     * TransformUtil.validateSchema() test
     */
    @Test
    void testValidateSchema() {
        // correct tables & invalid tables -> gkg, events, mentions
        LocalDateTime dt = LocalDateTime.of(2026, 10, 22, 10, 30, 00);
        for (Map.Entry<String, Dataset<Row>> tableEntry : mockTables.entrySet()) {
            ValidationResult res = validateSchema(
                    tableEntry.getValue(),
                    tableEntry.getKey(),
                    dt);

            long invalidCount = res.invalid().count();
            if (invalidCount > 0) {
                String[] columns = res.invalid().columns();
                for (Row row : res.invalid().limit(5).collectAsList()) {
                    System.out.println("---- Invalid Row ----");
                    for (int i = 0; i < columns.length; i++) {
                        System.out.printf("%-25s : %s%n", columns[i], row.get(i));
                    }
                }

                // res.invalid().show(false);
            }
            // testData.json doesn't have invalid columns so invalid rows should be 0
            assertEquals(0, invalidCount);
        }

    }

    @Test
    void testNormalizeTable() throws URISyntaxException, IOException {
        // normalize mockTables -> assert with correctTables
        LocalDateTime dt = LocalDateTime.of(2026, 10, 22, 10, 30, 00);
        for (Map.Entry<String, Dataset<Row>> tableEntry : mockTables.entrySet()) {
            ValidationResult validationResult = validateSchema(
                    tableEntry.getValue(),
                    tableEntry.getKey(),
                    dt);

            Map<String, Dataset<Row>> resultTables = normalizeTable(validationResult.valid(), tableEntry.getKey());
            Map<String, Dataset<Row>> tfProduct = correctTables.get(tableEntry.getKey());
            for (Map.Entry<String, Dataset<Row>> resultEntry : resultTables.entrySet()) {
                Dataset<Row> result = resultEntry.getValue();
                String tableName = resultEntry.getKey();
                Dataset<Row> correct = tfProduct.get(tableName);

                StructField[] actualFields = result.schema().fields();
                StructField[] expectedFields = Schema.schemaMap.get(tableName).fields();
                try {
                    assertEquals(expectedFields.length, actualFields.length);
                } catch (AssertionFailedError e) {
                    System.out.println("Result:");
                    result.printSchema();
                    System.out.println("Correct:");
                    Schema.schemaMap.get(tableName).printTreeString();
                    throw e;
                }

                try {
                    for (int i = 0; i < expectedFields.length; i++) {
                        assertEquals(expectedFields[i].name(), actualFields[i].name());
                        assertTrue(
                                DataType.equalsIgnoreNullability(
                                        expectedFields[i].dataType(),
                                        actualFields[i].dataType()),
                                String.format(
                                        "Column %s: expected %s, actual %s",
                                        expectedFields[i].name(),
                                        expectedFields[i].dataType(),
                                        actualFields[i].dataType()));
                    }
                } catch (AssertionFailedError e) {
                    System.out.println("Raw table:");
                    tableEntry.getValue().printSchema();
                    System.out.println("Result:");
                    result.printSchema();
                    System.out.println("Correct:");
                    Schema.schemaMap.get(tableName).printTreeString();
                    throw e;
                }

                // convert map to sorted array for set ops
                if (tableName.equals("silver_gkg_gcam")) {
                    result = result.withColumn("gcam_records", sort_array(map_entries(col("gcam_records"))));
                    correct = correct.withColumn("gcam_records", sort_array(map_entries(col("gcam_records"))));
                }
                Dataset<Row> resultCorrectDiff = result.exceptAll(correct);
                Dataset<Row> correctResultDiff = correct.exceptAll(result);
                try {
                    assertTrue(resultCorrectDiff.isEmpty(), "Rows are not equal");
                    assertTrue(correctResultDiff.isEmpty(), "Rows are not equal");
                } catch (AssertionFailedError e) {
                    System.out.println("Row diff of (result - correct):");
                    resultCorrectDiff.show(10, 0, true);
                    System.out.println("Row diff of (correct - result):");
                    correctResultDiff.show(10, 0, true);
                    throw e;
                }
            }
        }
    }

    @Test
    // @SuppressWarnings("unchecked")
    void testFlushInvalidRows() {
        // gonna need the minio fixture too
        // check if flushed files are there
        Dataset<Row> invalidDf = spark.createDataFrame(
                List.of(
                        RowFactory.create(1, "test1", "20261120", Arrays.asList("error1", "error2", "error3")),
                        RowFactory.create(2, "test2", "20261120", Arrays.asList("error2", "error3")),
                        RowFactory.create(3, "test3", "20261120", Arrays.asList("error3"))),
                new StructType()
                        .add("id", "int")
                        .add("name", "string")
                        .add("pub_date", "string")
                        .add("_validation_errors", "array<string>"));
        String tableName = "mockTable";
        String outputPath = tempDir.resolve("mock-bucket/invalid/test-stage/mock-table").toString();
        Map<String, Object> result = flushInvalidRows(invalidDf, tableName, outputPath);

        assertEquals(Long.valueOf(3), result.get("invalid_row_count"));
        Map<?, ?> raw = (Map<?, ?>) result.get("validation_errors");
        Map<String, Long> valErrors = new HashMap<>();
        raw.forEach((k, v) -> {
            valErrors.put((String) k, (Long) v);
        });
        assertEquals(Long.valueOf(1), valErrors.get("error1"));
        assertEquals(Long.valueOf(2), valErrors.get("error2"));
        assertEquals(Long.valueOf(3), valErrors.get("error3"));
    }

    @Test
    void testMergeLookupTable() {
        // gonna need an old lookup table and new lookup table
        // that has overlapping data, and the resulting data
        // compare and assert equal
        Dataset<Row> newDF = spark.createDataFrame(
                List.of(
                        RowFactory.create("test2", Timestamp.valueOf("2026-11-23 16:30:00")),
                        RowFactory.create("test3", Timestamp.valueOf("2026-11-23 16:45:00"))),
                new StructType()
                        .add("document_id", "string")
                        .add("file_datetime", "timestamp"));

        Dataset<Row> existingDF = spark.createDataFrame(
                List.of(
                        RowFactory.create("test1", Timestamp.valueOf("2026-11-23 16:00:00")),
                        RowFactory.create("test2", Timestamp.valueOf("2026-11-23 16:15:00"))),
                new StructType()
                        .add("document_id", "string")
                        .add("file_datetime", "timestamp"));

        Dataset<Row> mergedActual = mergeLookupTable(newDF, existingDF);
        Dataset<Row> expected = spark.createDataFrame(
                List.of(
                        RowFactory.create("test1", Timestamp.valueOf("2026-11-23 16:00:00")),
                        RowFactory.create("test2", Timestamp.valueOf("2026-11-23 16:30:00")),
                        RowFactory.create("test3", Timestamp.valueOf("2026-11-23 16:45:00"))),
                new StructType()
                        .add("document_id", "string")
                        .add("file_datetime", "timestamp"));

        assertEquals(expected.schema(), mergedActual.schema());

        assertEquals(expected.exceptAll(mergedActual).count(), 0L);

        assertEquals(mergedActual.exceptAll(expected).count(), 0L);
    }
}
