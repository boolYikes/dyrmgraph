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
    // TODO: probably needs refactoring 😂
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
                // NOTE: this will be repartitioned in the compaction workflow
                pmod(hash(col("V2SourceCollectionIdentifier"),
                        col("V2DocumentIdentifier")), lit(128)).alias("partition_hash"));

        Column toneArray = split(col("V1_5Tone"), ",");
        Dataset<Row> silver_gkg = input.select(
                col("GKGRecordID").alias("gkg_id"),
                concat_ws(
                        "|",
                        col("V2SourceCollectionIdentifier"),
                        col("V2DocumentIdentifier")).alias("document_id"),
                // Exploded V1.5Tone nested columns
                toneArray.getItem(0).cast("double").alias("tone"),
                toneArray.getItem(1).cast("double").alias("positive_score"),
                toneArray.getItem(2).cast("double").alias("negative_score"),
                toneArray.getItem(3).cast("double").alias("polarity"),
                toneArray.getItem(4).cast("double").alias("activity_reference_density"),
                toneArray.getItem(5).cast("double").alias("self_group_reference_density"),
                toneArray.getItem(6).cast("int").alias("word_count"),
                // Kept as is (as string arrays) because of usage ambiguity
                split(col("V2_1RelatedImages"), ";").alias("related_images"),
                split(col("V2_1SocialImageEmbeds"), ";").alias("social_image_embdes"),
                split(col("V2_1SocialVideoEmbeds"), ";").alias("social_video_embdes"),
                split(col("V2_1TranslationInfo"), ";").alias("translation_info"),
                // Maybe normalize later as its usefulness emerges
                col("V2ExtraSXML").alias("extras_xml"),
                // use file-name-provided date because v2.1date can contain 0
                to_date(col("pub_date"), "yyyyMMdd").alias("partition_date"));

        // Precompute canonical location id
        Column countExploded = explode(split(col("V2_1Counts"), ";")).alias("count_exp");
        Column countRecord = split(countExploded, "#", -1);
        Column countLocationKey = when(
                countRecord.getItem(9).isNotNull().and(not(countRecord.getItem(9).equalTo(""))),
                concat(lit("fid:"), countRecord.getItem(9))).otherwise(
                        concat_ws(
                                ":", lit("geo"),
                                coalesce(countRecord.getItem(3), lit("NULL")),
                                coalesce(countRecord.getItem(4), lit("NULL")),
                                coalesce(countRecord.getItem(5), lit("NULL")),
                                coalesce(countRecord.getItem(6), lit("NULL")),
                                coalesce(countRecord.getItem(7), lit("NULL")),
                                coalesce(countRecord.getItem(8), lit("NULL")))
                                .alias("location_computed_id"));
        Dataset<Row> silver_gkg_counts = input.select(
                // partition date + gkg_id + offset
                concat_ws(
                        "|",
                        col("pub_date"),
                        col("GKGRecordID"),
                        countRecord.getItem(10)).alias("count_computed_id"),
                to_date(col("pub_date"), "yyyyMMdd").alias("partition_date"),
                col("GKGRecordID").alias("gkg_id"),
                countRecord.getItem(0).alias("count_type"),
                countRecord.getItem(1).cast("long").alias("count"),
                countRecord.getItem(2).alias("object_type"),
                countLocationKey,
                countRecord.getItem(10).cast("int").alias("offset"));

        Column locationExploded = explode(split(col("V2EnhancedLocations"), ";")).alias("loc_exploded");
        Column locationRecord = split(locationExploded, "#", -1);
        Column locationKey = when(
                locationRecord.getItem(7).isNotNull().and(locationRecord.getItem(7).equalTo("")),
                concat(lit("fid:"), locationRecord.getItem(7))).otherwise(
                        // index 4 is ADM2: loc compute key doesn't universally have it
                        // for now, keep it out of the copmuted key.
                        concat_ws(
                                ":",
                                lit("geo"),
                                coalesce(countRecord.getItem(0), lit("NULL")),
                                coalesce(countRecord.getItem(1), lit("NULL")),
                                coalesce(countRecord.getItem(2), lit("NULL")),
                                coalesce(countRecord.getItem(3), lit("NULL")),
                                coalesce(countRecord.getItem(5), lit("NULL")),
                                coalesce(countRecord.getItem(6), lit("NULL"))))
                .alias("location_computed_id");
        Dataset<Row> silver_gkg_locations = input.select(
                locationKey,
                concat_ws(
                        "|",
                        col("GKGRecordID"),
                        locationRecord.getItem(8)).alias("gkg_location_id"), // it's a PK but I might not need it.
                to_date(col("pub_date"), "yyyyMMdd").alias("partition_date"),
                col("GKGRecordID").alias("gkg_id"),
                locationRecord.getItem(0).alias("type"),
                locationRecord.getItem(1).alias("fullname"),
                locationRecord.getItem(2).alias("country_code"),
                locationRecord.getItem(3).alias("adm1_code"),
                locationRecord.getItem(4).alias("adm2_code"),
                locationRecord.getItem(5).cast("double").alias("latitude"),
                locationRecord.getItem(6).cast("double").alias("longitude"),
                locationRecord.getItem(7).alias("feature_id"),
                locationRecord.getItem(8).cast("int").alias("offset"));

        Column themeExploded = explode(split(col("V2EnhancedThemes"), ";"));
        Column themeRecord = split(themeExploded, ",");
        Dataset<Row> silver_gkg_themes = input.select(
                col("GKGRecordID").alias("gkg_id"),
                to_date(col("pub_date"), "yyyyMMdd").alias("partition_date"),
                themeRecord.getItem(0).alias("theme"),
                themeRecord.getItem(1).cast("int").alias("offset"));

        Column personExploded = explode(split(col("V2EnhancedPersons"), ";"));
        Column personRecord = split(personExploded, ",");
        Dataset<Row> silver_gkg_persons = input.select(
                col("GKGRecordID").alias("gkg_id"),
                to_date(col("pub_date"), "yyyyMMdd").alias("partition_date"),
                personRecord.getItem(0).alias("person"),
                personRecord.getItem(1).cast("int").alias("offset"));

        Column orgExploded = explode(split(col("V2EnhancedOrganizations"), ";"));
        Column orgRecord = split(orgExploded, ",");
        Dataset<Row> silver_gkg_organizations = input.select(
                col("GKGRecordID").alias("gkg_id"),
                to_date(col("pub_date"), "yyyyMMdd").alias("partition_date"),
                orgRecord.getItem(0).alias("organization"),
                orgRecord.getItem(1).cast("int").alias("offset"));

        Column dateExploded = explode(split(col("V2_1EnhancedDates"), ";"));
        Column dateRecord = split(dateExploded, ",");
        Dataset<Row> silver_gkg_dates = input.select(
                col("GKGRecordID").alias("gkg_id"),
                to_date(col("pub_date"), "yyyyMMdd").alias("partition_date"),
                dateRecord.getItem(0).cast("int").alias("date_resolution"),
                dateRecord.getItem(1).cast("int").alias("month"),
                dateRecord.getItem(2).cast("int").alias("day"),
                dateRecord.getItem(3).cast("int").alias("year"),
                dateRecord.getItem(4).cast("int").alias("offset"));

        // TODO: NOPE. GCAM is too wide. Keep this as a MapType<>
        Column gcamExploded = explode(split(col("V2GCAM"), ","));
        Column gcamRecord = split(gcamExploded, ":");
        Dataset<Row> silver_gkg_gcam = input.select(
                col("GKGRecordID").alias("gkg_id"),
                to_date(col("pub_date"), "yyyyMMdd").alias("partition_date"),
                split(gcamRecord.getItem(0), ".").alias("year"));

        Dataset<Row> silver_gkg_quotations = input.select(
                col("GKGRecordID").alias("gkg_id"),
                to_date(col("pub_date"), "yyyyMMdd").alias("partition_date"));
        Dataset<Row> silver_gkg_all_names = input.select(
                col("GKGRecordID").alias("gkg_id"),
                to_date(col("pub_date"), "yyyyMMdd").alias("partition_date"));
        Dataset<Row> silver_gkg_amounts = input.select(
                col("GKGRecordID").alias("gkg_id"),
                to_date(col("pub_date"), "yyyyMMdd").alias("partition_date"));

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
