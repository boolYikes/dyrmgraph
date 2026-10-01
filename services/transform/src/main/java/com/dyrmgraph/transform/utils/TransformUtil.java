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
    private static Dataset<Row> extractGKG_GCAM(Dataset<Row> input) {
        // NOTE: GCAM is too wide. Keep this as a MapType<>
        // NOTE: GCAM lookup table:
        // https://data.gdeltproject.org/documentation/GCAM-MASTER-CODEBOOK.TXT
        Column gcamTransformed = transform(
                split(col("V2GCAM"), ","),
                x -> {
                    Column entry = split(x, ":");
                    return struct(
                            entry.getItem(0),
                            entry.getItem(1).cast("double"));
                });
        return input.select(
                col("GKGRecordID").alias("gkg_id"),
                to_date(col("pub_date"), "yyyyMMdd").alias("partition_date"),
                map_from_entries(gcamTransformed).alias("gcam_records"));
    }

    private static Dataset<Row> extractDocuments(Dataset<Row> input) {
        return input.select(
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
    }

    private static Dataset<Row> extractGKG(Dataset<Row> input) {
        Column toneArray = split(col("V1_5Tone"), ",");
        return input.select(
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
    }

    private static Dataset<Row> extractGKG_Locations(Dataset<Row> input) {
        Dataset<Row> locationExploded = input.withColumn(
                "location_exploded",
                explode(split(col("V2EnhancedLocations"), ";")));
        Column locationRecord = split(col("location_exploded"), "#", -1);
        Column locationKey = when(
                locationRecord.getItem(7).isNotNull().and(not(locationRecord.getItem(7).equalTo(""))),
                concat(lit("fid:"), locationRecord.getItem(7))).otherwise(
                        // index 4 is ADM2: loc compute key doesn't universally have it
                        // for now, keep it out of the copmuted key.
                        concat_ws(
                                ":",
                                lit("geo"),
                                coalesce(locationRecord.getItem(0), lit("NULL")),
                                coalesce(locationRecord.getItem(1), lit("NULL")),
                                coalesce(locationRecord.getItem(2), lit("NULL")),
                                coalesce(locationRecord.getItem(3), lit("NULL")),
                                coalesce(locationRecord.getItem(5), lit("NULL")),
                                coalesce(locationRecord.getItem(6), lit("NULL"))))
                .alias("location_computed_id");
        return locationExploded.select(
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
    }

    private static Dataset<Row> extractGKG_Counts(Dataset<Row> input) {
        // Precompute canonical location id
        Dataset<Row> countExploded = input.withColumn(
                "count_exploded",
                explode(split(col("V2_1Counts"), ";")));
        Column countRecord = split(col("count_exploded"), "#", -1);
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
                                coalesce(countRecord.getItem(8), lit("NULL"))))
                .alias("location_computed_id");
        return countExploded.select(
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
    }

    /* TPO is a short for Theme, Person, Organization */
    private static Dataset<Row> extractTPO(Dataset<Row> input, String target) {
        Map<String, String> colNameMap = Map.of(
                "theme", "V2EnhancedThemes",
                "person", "V2EnhancedPersons",
                "organization", "V2EnhancedOrganizations");
        Dataset<Row> exploded = input.withColumn(
                "exploded",
                explode(split(col(colNameMap.get(target)), ";")));
        Column themeRecord = split(col("exploded"), ",");
        return exploded.select(
                col("GKGRecordID").alias("gkg_id"),
                to_date(col("pub_date"), "yyyyMMdd").alias("partition_date"),
                themeRecord.getItem(0).alias(target),
                themeRecord.getItem(1).cast("int").alias("offset"));
    }

    private static Dataset<Row> extractGKG_Dates(Dataset<Row> input) {
        Dataset<Row> dateExploded = input.withColumn(
                "date_exploded",
                explode(split(col("V2_1EnhancedDates"), ";")));
        Column dateRecord = split(col("date_exploded"), ",");
        return dateExploded.select(
                col("GKGRecordID").alias("gkg_id"),
                to_date(col("pub_date"), "yyyyMMdd").alias("partition_date"),
                dateRecord.getItem(0).cast("int").alias("date_resolution"),
                dateRecord.getItem(1).cast("int").alias("month"),
                dateRecord.getItem(2).cast("int").alias("day"),
                dateRecord.getItem(3).cast("int").alias("year"),
                dateRecord.getItem(4).cast("int").alias("offset"));

    }

    private static Dataset<Row> extractGKG_quotations(Dataset<Row> input) {
        Dataset<Row> quotationExploded = input.withColumn(
                "quotation_exploded",
                explode(split(col("V2_1Quotations"), "#")));
        Column quotationRecord = split(col("quoatation_exploded"), "|");

        return quotationExploded.select(
                col("GKGRecordID").alias("gkg_id"),
                to_date(col("pub_date"), "yyyyMMdd").alias("partition_date"),
                quotationRecord.getItem(0).cast("int").alias("offset"),
                quotationRecord.getItem(1).cast("int").alias("length"),
                quotationRecord.getItem(2).alias("verb"),
                quotationRecord.getItem(3).alias("quote"));
    }

    private static Dataset<Row> extractGKG_AllNames(Dataset<Row> input) {
        Dataset<Row> allNamesExploded = input.withColumn(
                "all_names_exploded",
                explode(split(col("V2_1AllNames"), ";")));
        Column allNamesRecord = split(col("all_names_exploded"), ",");
        return allNamesExploded.select(
                col("GKGRecordID").alias("gkg_id"),
                to_date(col("pub_date"), "yyyyMMdd").alias("partition_date"),
                allNamesRecord.getItem(0).alias("name"),
                allNamesRecord.getItem(1).cast("int").alias("offset"));
    }

    private static Dataset<Row> extractGKG_Amounts(Dataset<Row> input) {
        Dataset<Row> amountsExploded = input.withColumn(
                "amounts_exploded",
                explode(split(col("V2_1Amounts"), ";")));
        Column amountsRecord = split(col("amounts_exploded"), ",");
        return amountsExploded.select(
                col("GKGRecordID").alias("gkg_id"),
                to_date(col("pub_date"), "yyyyMMdd").alias("partition_date"),
                amountsRecord.getItem(0).cast("double").alias("amount"),
                amountsRecord.getItem(1).alias("object"),
                amountsRecord.getItem(2).cast("int").alias("offset"));
    }

    private static Map<String, Dataset<Row>> normalizeGKG(Dataset<Row> input) {
        // tables to extract: silver_gkg
        // variables will follow actual table names conventions
        Map<String, Dataset<Row>> tableMap = new HashMap<>();
        tableMap.put("silver_documents", extractDocuments(input));
        tableMap.put("silver_gkg", extractGKG(input));
        tableMap.put("silver_gkg_counts", extractGKG_Counts(input));
        tableMap.put("silver_gkg_locations", extractGKG_Locations(input));
        tableMap.put("silver_gkg_themes", extractTPO(input, "theme"));
        tableMap.put("silver_gkg_persons", extractTPO(input, "person"));
        tableMap.put("silver_gkg_organizations", extractTPO(input, "organization"));
        tableMap.put("silver_gkg_dates", extractGKG_Dates(input));
        tableMap.put("silver_gkg_gcam", extractGKG_GCAM(input));
        tableMap.put("silver_gkg_quotations", extractGKG_quotations(input));
        tableMap.put("silver_gkg_all_names", extractGKG_AllNames(input));
        tableMap.put("silver_gkg_amounts", extractGKG_Amounts(input));
        return tableMap;
    }

    /* This is consumed by Neo4j loader downstream */
    private static Dataset<Row> extractActorsAndAction(Dataset<Row> input) {
        return input.select(
                col("GlobalEventID").alias("event_id"),
                // actor 1
                col("Actor1Code").alias("actor1_code"),
                col("Actor1Name").alias("actor1_name"),
                col("Actor1CountryCode").alias("actor1_country_code"),
                col("Actor1KnownGroupCode").alias("actor1_known_group_code"),
                col("Actor1EthnicCode").alias("actor1_ethnic_code"),
                col("Actor1Religion1Code").alias("actor1_religion1_code"),
                col("Actor1Religion2Code").alias("actor1_religion2_code"),
                col("Actor1Type1Code").alias("actor1_type1_code"),
                col("Actor1Type2Code").alias("actor1_type2_code"),
                col("Actor1Type3Code").alias("actor1_type3_code"),
                // actor1 geo
                col("Actor1GeoType").cast("int").alias("actor1_geo_type"),
                col("Actor1GeoFullName").alias("actor1_geo_fullname"),
                col("Actor1GeoCountryCode").alias("actor1_geo_country_code"),
                col("Actor1GeoADM1Code").alias("actor1_geo_adm1_code"),
                col("Actor1GeoADM2Code").alias("actor1_geo_adm2_code"),
                col("Actor1GeoLat").cast("double").alias("actor1_geo_lat"),
                col("Actor1GeoLong").cast("double").alias("actor1_geo_long"),
                col("Actor1GeoFeatureID").alias("actor1_geo_feature_id"),
                // actor 2
                col("Actor2Code").alias("actor2_code"),
                col("Actor2Name").alias("actor2_name"),
                col("Actor2CountryCode").alias("actor2_country_code"),
                col("Actor2KnownGroupCode").alias("actor2_known_group_code"),
                col("Actor2EthnicCode").alias("actor2_ethnic_code"),
                col("Actor2Religion1Code").alias("actor2_religion1_code"),
                col("Actor2Religion2Code").alias("actor2_religion2_code"),
                col("Actor2Type1Code").alias("actor2_type1_code"),
                col("Actor2Type2Code").alias("actor2_type2_code"),
                col("Actor2Type3Code").alias("actor2_type3_code"),
                // actor2 geo
                col("Actor2GeoType").cast("int").alias("actor2_geo_type"),
                col("Actor2GeoFullName").alias("actor2_geo_fullname"),
                col("Actor2GeoCountryCode").alias("actor2_geo_country_code"),
                col("Actor2GeoADM1Code").alias("actor2_geo_adm1_code"),
                col("Actor2GeoADM2Code").alias("actor2_geo_adm2_code"),
                col("Actor2GeoLat").cast("double").alias("actor2_geo_lat"),
                col("Actor2GeoLong").cast("double").alias("actor2_geo_long"),
                col("Actor2GeoFeatureID").alias("actor2_geo_feature_id"),
                // action
                col("IsRootEvent").equalTo("1").alias("is_root_event"),
                col("EventCode").alias("event_code"),
                col("EventBaseCode").alias("event_base_code"),
                col("EventRootCode").alias("event_root_code"),
                col("QuadClass").cast("int").alias("quad_class"),
                col("GoldsteinScale").cast("double").alias("goldstein_scale"),
                // action geo
                col("ActionGeoType").cast("int").alias("action_geo_type"),
                col("ActionGeoFullName").alias("action_geo_fullname"),
                col("ActionGeoCountryCode").alias("action_geo_country_code"),
                col("ActionGeoADM1Code").alias("action_geo_adm1_code"),
                col("ActionGeoADM2Code").alias("action_geo_adm2_code"),
                col("ActionGeoLat").cast("double").alias("action_geo_lat"),
                col("ActionGeoLong").cast("double").alias("action_geo_long"),
                col("ActionGeoFeatureID").alias("action_geo_feature_id"));
    }

    private static Map<String, Dataset<Row>> normalizeEvents(Dataset<Row> input) {
        Map<String, Dataset<Row>> tableMap = new HashMap<>();
        Dataset<Row> silver_events = input.select(
                col("GlobalEventID").alias("event_id"),
                col("Day").cast("int").alias("day"),
                col("MonthYear").cast("int").alias("month_year"),
                col("Year").cast("int").alias("year"),
                col("FractionDate").cast("double").alias("fraction_date"),
                to_date(col("DateAdded"), "yyyyMMddHHmmss").alias("date_added"),
                col("SourceURL").alias("source_url"));
        tableMap.put("silver_events", silver_events);
        tableMap.put("silver_event_network", extractActorsAndAction(input));
        return tableMap;
    }

    private static Map<String, Dataset<Row>> normalizeMentions(Dataset<Row> input) {
        Map<String, Dataset<Row>> tableMap = new HashMap<>();
        Dataset<Row> silver_mentions = input.select(
                concat_ws(
                        "|",
                        col("MentionType"),
                        col("MentionIdentifier"))
                        .alias("document_id"),
                col("GlobalEventID").alias("event_id"),
                to_date(col("pub_date"), "yyyyMMdd").alias("partition_date"),
                to_date(col("EventTimeDate"), "yyyyMMddHHmmss").alias("event_time_date"),
                to_date(col("MentionTimeDate"), "yyyyMMddHHmmss").alias("mention_time_date"),
                col("MentionType").alias("mention_type"),
                col("MentionSourceName").alias("mention_source_name"),
                col("MentionIdentifier").alias("mention_identifier"),
                col("SentenceID").cast("int").alias("sentence_id"),
                col("InRawText").equalTo("1").alias("in_raw_text"), // type safety. casting bindly can backfire
                col("Confidence").cast("int").alias("confidence"),
                col("MentionDocLen").cast("int").alias("mention_doc_len"),
                col("MentionDocTone").cast("double").alias("mention_doc_tone"),
                col("Extras").alias("extras"), // currently unused, apparently.
                col("Actor1CharOffset").cast("int").alias("actor1_char_offset"),
                col("Actor2CharOffset").cast("int").alias("actor2_char_offset"),
                col("ActionCharOffset").cast("int").alias("action_char_offset"));
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
