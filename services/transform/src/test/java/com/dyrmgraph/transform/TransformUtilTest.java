/**
 * Component tests for actual transformation logic
 */
package com.dyrmgraph.transform;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import org.junit.jupiter.api.Test;

import com.dyrmgraph.transform.utils.TransformUtil.ValidationResult;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;
import java.time.LocalDateTime;
import static com.dyrmgraph.transform.utils.TransformUtil.*;

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
        for (Map.Entry<String, Dataset<Row>> tableEntry : mockTables.entrySet()) {
            LocalDateTime dt = LocalDateTime.of(2026, 10, 22, 10, 30, 00);
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
    void testNormalizeGKG() {
        // have a correct data json ready and assert equal at the end of the test.
    }

    @Test
    void testNormalizeEvents() {

    }

    @Test
    void testNormalizeMentions() {

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
