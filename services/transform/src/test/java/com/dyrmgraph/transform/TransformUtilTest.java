/**
 * Component tests for actual transformation logic
 */
package com.dyrmgraph.transform;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.DataType;
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
import java.io.IOException;
import java.time.LocalDateTime;
import java.net.URISyntaxException;

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
    void testComputedKeysAreCorrect() {
        // test computed keys for locations, counts, etc...
    }

    @Test
    void testFlushInvalidRows() {
        // gonna need the minio fixture too
        // check if flushed files are there
    }

    @Test
    void testMergeLookupTable() {
        // gonna need an old lookup table and new lookup table
        // that has overlapping data, and the resulting data
        // compare and assert equal
    }
}
