package com.dyrmgraph.transform.utils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Column;
import static org.apache.spark.sql.functions.*;

import java.time.LocalDate;
import java.util.Map;
import java.util.HashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class TransformUtil {

    private static Map<String, Function<Dataset<Row>, Dataset<Row>>> yup = Map.of(
            "gkg", TransformUtil::normalizeGKG,
            "events", TransformUtil::normalizeEvents,
            "mentions", TransformUtil::normalizeMentions);

    private TransformUtil() {
    }

    public record ValidationResult(
            Dataset<Row> valid,
            Dataset<Row> invalid) {
    }

    private static Dataset<Row> validate(Dataset<Row> input, String tableName) {
        // java generator! neat
        Column errors = array(
                Schema.regexMap.get(tableName).entrySet().stream()
                        .map(e -> when(not(col(e.getKey()).rlike(e.getValue())), lit(e.getKey())))
                        .toArray(Column[]::new));

        Dataset<Row> validated = input
                .withColumn("_validation_errors", errors)
                .withColumn("_validation_errors", expr("filter(_validation_errors, x -> x is not null)"));

        return validated;
    }

    private static Dataset<Row> normalizeGKG(Dataset<Row> input) {
        return input;
    }

    private static Dataset<Row> normalizeEvents(Dataset<Row> input) {
        return input;
    }

    private static Dataset<Row> normalizeMentions(Dataset<Row> input) {
        return input;
    }

    /**
     * Validates columns with regex patterns and return the valid parts of the df,
     * and logs the invalid rows if present
     * 
     * @param input
     * @param tableName
     * @return valid df
     */
    public static ValidationResult validateSchema(Dataset<Row> input, String tableName, LocalDate date) {
        Dataset<Row> validated = validate(input, tableName);
        validated = validated.withColumn("pub_date", lit(date));
        Dataset<Row> valid = validated.filter(size(col("_validation_errors")).equalTo(0));
        Dataset<Row> invalid = validated.filter(size(col("_validation_errors")).gt(0));

        return new ValidationResult(valid, invalid);
    }

    public static Map<String, Object> flushInvalidRows(Dataset<Row> invalidDF, String tableName, String outputPath) {
        // NOTE: no dupe lineage?
        // Dataset<Row> is not a static data but a query plan
        // Because of this, for each invalidDF.xx() invokation,
        // spark could re-compute invalidDF from the source
        // persist and unpersist prevents this
        invalidDF.persist();

        try {
            invalidDF.write().format("parquet")
                    .partitionBy("pub_date").mode("append")
                    .save(outputPath);

            long invalidRowCount = invalidDF.count();

            // Count each validation error
            Dataset<Row> errorCounts = invalidDF
                    .select(explode(col("_validation_errors")).alias("error"))
                    .groupBy("error")
                    .count();

            Map<String, Long> validationErrors = errorCounts
                    .collectAsList()
                    .stream()
                    .collect(Collectors.toMap(
                            row -> row.getString(0),
                            row -> row.getLong(1)));

            Map<String, Object> result = new HashMap<>();
            result.put("invalid_row_count", invalidRowCount);
            result.put("validation_errors", validationErrors);
            return result;

        } finally {
            invalidDF.unpersist();
        }
    }

    public static Map<String, Dataset<Row>> normalizeTables(Dataset<Row> input, String tableName) {
        // TODO: implement normalizexxx
        // 4.2 dedupe with unique ids
        // 4.3 explode nested cols
        Dataset<Row> clean = yup.get(tableName).apply(input);
        // TODO: return products from the parent table
        return Map.of(tableName, clean);
    }

}
