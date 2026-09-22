package com.dyrmgraph.transform.utils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Column;
import static org.apache.spark.sql.functions.*;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.HashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class TransformUtil {

    private static Map<String, Function<Dataset<Row>, Map<String, Dataset<Row>>>> yup = Map.of(
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

    // NOTE
    // Mentions depends on gkg transform status. maybe should load up both mentions
    // and gkg?
    // or gkg -> metions -> events, but if the computed id of the gkg table and the
    // mentions table does not match i have to abort it as it would lead to
    // incorrect rows insertion
    private static Map<String, Dataset<Row>> normalizeGKG(Dataset<Row> input) {
        // tables to extract: silver_gkg
        // variables will follow actual table names conventions
        Map<String, Dataset<Row>> tableMap = new HashMap<>();
        // TODO: implement these
        Dataset<Row> silver_documents = input.select(
                concat_ws(
                        "|",
                        col("V2SourceCollectionIdentifier"),
                        col("V2DocumentIdentifier"))
                        .alias("document_id"),
                col("V2DocumentIdentifier").alias("document_identifier"),
                col("V2SourceCollectionIdentifier").alias("source_collection_ident"),
                col("V2SourceCommonName").alias("source_common_name"),
                col("V2_1SharingImage").alias("sharing_image"),
                col(""));
        Dataset<Row> silver_gkg = input.select(
                col("GKGRecordID").alias("gkg_id"),
                col(""));
        Dataset<Row> silver_gkg_counts = input.select();
        Dataset<Row> silver_gkg_locations = input.select();
        Dataset<Row> silver_gkg_themes = input.select();
        Dataset<Row> silver_gkg_persons = input.select();
        Dataset<Row> silver_gkg_organizations = input.select();
        Dataset<Row> silver_gkg_dates = input.select();
        Dataset<Row> silver_gkg_gcam = input.select();
        Dataset<Row> silver_gkg_quotations = input.select();
        Dataset<Row> silver_gkg_all_names = input.select();
        Dataset<Row> silver_gkg_amounts = input.select();
        tableMap.put("silver_documents", silver_documents);
        tableMap.put("silver_gkg", silver_gkg);
        tableMap.put("silver_gkg_counts", silver_gkg_counts);
        tableMap.put("silver_gkg_locations", silver_gkg_locations);
        tableMap.put("silver_gkg_themes", silver_gkg_themes);
        tableMap.put("silver_gkg_persons", silver_gkg_persons);
        tableMap.put("silver_gkg_organizations", silver_gkg_organizations);
        tableMap.put("silver_gkg_dates", silver_gkg_dates);
        tableMap.put("silver_gkg_gcam", silver_gkg_gcam);
        tableMap.put("silver_gkg_quotations", silver_gkg_quotations);
        tableMap.put("silver_gkg_all_names", silver_gkg_all_names);
        tableMap.put("silver_gkg_amounts", silver_gkg_amounts);
        return tableMap;
    }

    // TODO: implement this
    private static Map<String, Dataset<Row>> normalizeEvents(Dataset<Row> input) {
        Map<String, Dataset<Row>> tableMap = new HashMap<>();
        Dataset<Row> silver_events = input.select();
        // NOTE: this part is for graphdb but for now...
        Dataset<Row> silver_event_action = input.select();
        Dataset<Row> silver_event_actor1 = input.select();
        Dataset<Row> silver_event_actor2 = input.select();
        tableMap.put("silver_events", silver_events);
        tableMap.put("silver_event_actions", silver_event_action);
        tableMap.put("silver_event_actor1", silver_event_actor1);
        tableMap.put("silver_event_actor2", silver_event_actor2);
        return tableMap;
    }

    // TODO: implement this
    private static Map<String, Dataset<Row>> normalizeMentions(Dataset<Row> input) {
        Map<String, Dataset<Row>> tableMap = new HashMap<>();
        Dataset<Row> silver_mentions = input.select();
        tableMap.put("silver_mentions", silver_mentions);
        return tableMap;
    }

    /**
     * Validates columns with regex patterns and return the valid parts of the df,
     * and logs the invalid rows if present
     * 
     * @param input
     * @param tableName
     * @return valid df
     */
    public static ValidationResult validateSchema(Dataset<Row> input, String tableName, LocalDateTime dt) {
        Dataset<Row> validated = validate(input, tableName);
        validated = validated.withColumn("pub_date", lit(dt));
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
            // NOTE: contrary to the docstring in collectAsList(), this is inexpensive
            // because the error column is low cardinality
            Dataset<Row> errorCounts = invalidDF
                    .select(explode(col("_validation_errors")).alias("error"))
                    .groupBy("error")
                    .count();

            Map<String, Long> validationErrors = errorCounts
                    .collectAsList()
                    .stream()
                    .collect(Collectors.toMap(
                            row -> row.getString(0), // same as getAs("error")
                            row -> row.getLong(1))); // same as getAs("count")

            Map<String, Object> result = new HashMap<>();
            result.put("invalid_row_count", invalidRowCount);
            result.put("validation_errors", validationErrors);
            return result;

        } finally {
            invalidDF.unpersist();
        }
    }

    public static Map<String, Dataset<Row>> normalizeTable(Dataset<Row> input, String tableName) {
        return yup.get(tableName).apply(input);
    }

}
