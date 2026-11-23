package com.dyrmgraph.transform.utils;

import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public final class Schema {
    private static final StructType GKG_SCHEMA = new StructType()
            .add("GKGRecordID", DataTypes.StringType, false)
            .add("V2_1Date", "string")
            .add("V2SourceCollectionIdentifier", "string")
            .add("V2SourceCommonName", "string")
            .add("V2DocumentIdentifier", "string")
            .add("V1Counts", "string")
            .add("V2_1Counts", "string")
            .add("V1Themes", "string")
            .add("V2EnhancedThemes", "string")
            .add("V1Locations", "string")
            .add("V2EnhancedLocations", "string")
            .add("V1Persons", "string")
            .add("V2EnhancedPersons", "string")
            .add("V1Organizations", "string")
            .add("V2EnhancedOrganizations", "string")
            .add("V1_5Tone", "string")
            .add("V2_1EnhancedDates", "string")
            .add("V2GCAM", "string")
            .add("V2_1SharingImage", "string")
            .add("V2_1RelatedImages", "string")
            .add("V2_1SocialImageEmbeds", "string")
            .add("V2_1SocialVideoEmbeds", "string")
            .add("V2_1Quotations", "string")
            .add("V2_1AllNames", "string")
            .add("V2_1Amounts", "string")
            .add("V2_1TranslationInfo", "string")
            .add("V2ExtraSXML", "string");

    private static final StructType EVENTS_SCHEMA = new StructType()
            .add("GlobalEventID", "string")
            .add("Day", "string")
            .add("MonthYear", "string")
            .add("Year", "string")
            .add("FractionDate", "string")
            .add("Actor1Code", "string")
            .add("Actor1Name", "string")
            .add("Actor1CountryCode", "string")
            .add("Actor1KnownGroupCode", "string")
            .add("Actor1EthnicCode", "string")
            .add("Actor1Religion1Code", "string")
            .add("Actor1Religion2Code", "string")
            .add("Actor1Type1Code", "string")
            .add("Actor1Type2Code", "string")
            .add("Actor1Type3Code", "string")
            .add("Actor2Code", "string")
            .add("Actor2Name", "string")
            .add("Actor2CountryCode", "string")
            .add("Actor2KnownGroupCode", "string")
            .add("Actor2EthnicCode", "string")
            .add("Actor2Religion1Code", "string")
            .add("Actor2Religion2Code", "string")
            .add("Actor2Type1Code", "string")
            .add("Actor2Type2Code", "string")
            .add("Actor2Type3Code", "string")
            .add("IsRootEvent", "string")
            .add("EventCode", "string")
            .add("EventBaseCode", "string")
            .add("EventRootCode", "string")
            .add("QuadClass", "string")
            .add("GoldsteinScale", "string")
            .add("NumMentions", "string")
            .add("NumSources", "string")
            .add("NumArticles", "string")
            .add("AvgTone", "string")
            .add("Actor1GeoType", "string")
            .add("Actor1GeoFullName", "string")
            .add("Actor1GeoCountryCode", "string")
            .add("Actor1GeoADM1Code", "string")
            .add("Actor1GeoADM2Code", "string")
            .add("Actor1GeoLat", "string")
            .add("Actor1GeoLong", "string")
            .add("Actor1GeoFeatureID", "string")
            .add("Actor2GeoType", "string")
            .add("Actor2GeoFullName", "string")
            .add("Actor2GeoCountryCode", "string")
            .add("Actor2GeoADM1Code", "string")
            .add("Actor2GeoADM2Code", "string")
            .add("Actor2GeoLat", "string")
            .add("Actor2GeoLong", "string")
            .add("Actor2GeoFeatureID", "string")
            .add("ActionGeoType", "string")
            .add("ActionGeoFullName", "string")
            .add("ActionGeoCountryCode", "string")
            .add("ActionGeoADM1Code", "string")
            .add("ActionGeoADM2Code", "string")
            .add("ActionGeoLat", "string")
            .add("ActionGeoLong", "string")
            .add("ActionGeoFeatureID", "string")
            .add("DateAdded", "string")
            .add("SourceURL", "string");

    private static final StructType MENTIONS_SCHEMA = new StructType()
            .add("GlobalEventID", "string")
            .add("EventTimeDate", "string")
            .add("MentionTimeDate", "string")
            .add("MentionType", "string")
            .add("MentionSourceName", "string")
            .add("MentionIdentifier", "string")
            .add("SentenceID", "string")
            .add("Actor1CharOffset", "string")
            .add("Actor2CharOffset", "string")
            .add("ActionCharOffset", "string")
            .add("InRawText", "string")
            .add("Confidence", "string")
            .add("MentionDocLen", "string")
            .add("MentionDocTone", "string")
            .add("MentionDocTranslationInfo", "string")
            .add("Extras", "string");
    // transformed schemas
    private static final StructType SILVER_GKG_QUOTATIONS_SCHEMA = new StructType()
            .add("gkg_id", "string", false)
            .add("partition_date", "date", false)
            .add("batch_time", "string", false)
            .add("offset", "integer")
            .add("length", "integer")
            .add("verb", "string")
            .add("quote", "string");
    private static final StructType SILVER_GKG_SCHEMA = new StructType()
            .add("gkg_id", "string", false)
            .add("document_id", "string", false)
            .add("tone", "double")
            .add("positive_score", "double")
            .add("negative_score", "double")
            .add("polarity", "double")
            .add("activity_reference_density", "double")
            .add("self_group_reference_density", "double")
            .add("word_count", "int")
            .add("related_images", DataTypes.createArrayType(DataTypes.StringType, false))
            .add("social_image_embeds", DataTypes.createArrayType(DataTypes.StringType, false))
            .add("social_video_embeds", DataTypes.createArrayType(DataTypes.StringType, false))
            .add("translation_info", DataTypes.createArrayType(DataTypes.StringType, false))
            .add("extras_xml", "string")
            .add("partition_date", "date", false)
            .add("batch_time", "string", false);
    private static final StructType SILVER_GKG_GCAM_SCHEMA = new StructType()
            .add("gkg_id", "string", false)
            .add("partition_date", "date", false)
            .add("batch_time", "string", false)
            .add("gcam_records", "map<string, double>");
    private static final StructType SILVER_DOCUMENTS_SCHEMA = new StructType()
            .add("document_id", "string", false)
            .add("document_identifier", "string")
            .add("source_collection_ident", "string")
            .add("source_common_name", "string")
            .add("sharing_image", "string")
            .add("partition_hash", "int", false);
    private static final StructType SILVER_GKG_LOCATIONS_SCHEMA = new StructType()
            .add("location_computed_id", "string", false)
            .add("gkg_location_id", "string", false)
            .add("partition_date", "date", false)
            .add("batch_time", "string", false)
            .add("gkg_id", "string", false)
            .add("type", "string")
            .add("fullname", "string")
            .add("country_code", "string")
            .add("adm1_code", "string")
            .add("adm2_code", "string")
            .add("latitude", "double")
            .add("longitude", "double")
            .add("feature_id", "string")
            .add("offset", "int");
    private static final StructType SILVER_GKG_COUNTS_SCHEMA = new StructType()
            .add("count_computed_id", "string", false)
            .add("partition_date", "date", false)
            .add("batch_time", "string", false)
            .add("gkg_id", "string", false)
            .add("count_type", "string")
            .add("count", "long")
            .add("object_type", "string")
            .add("location_computed_id", "string")
            .add("offset", "int");
    private static final StructType SILVER_GKG_THEMES_SCHEMA = new StructType()
            .add("gkg_id", "string", false)
            .add("partition_date", "date", false)
            .add("batch_time", "string", false)
            .add("theme", "string")
            .add("offset", "int");
    private static final StructType SILVER_GKG_PERSONS_SCHEMA = new StructType()
            .add("gkg_id", "string", false)
            .add("partition_date", "date", false)
            .add("batch_time", "string", false)
            .add("person", "string")
            .add("offset", "int");
    private static final StructType SILVER_GKG_ORGANIZATIONS_SCHEMA = new StructType()
            .add("gkg_id", "string", false)
            .add("partition_date", "date", false)
            .add("batch_time", "string", false)
            .add("organization", "string")
            .add("offset", "int");
    private static final StructType SILVER_GKG_DATES_SCHEMA = new StructType()
            .add("gkg_id", "string", false)
            .add("partition_date", "date", false)
            .add("batch_time", "string", false)
            .add("date_resolution", "int")
            .add("month", "int")
            .add("day", "int")
            .add("year", "int")
            .add("offset", "int");
    private static final StructType SILVER_GKG_ALL_NAMES_SCHEMA = new StructType()
            .add("gkg_id", "string", false)
            .add("partition_date", "date", false)
            .add("batch_time", "string", false)
            .add("name", "string")
            .add("offset", "int");
    private static final StructType SILVER_GKG_AMOUNTS_SCHEMA = new StructType()
            .add("gkg_id", "string", false)
            .add("partition_date", "date", false)
            .add("batch_time", "string", false)
            .add("amount", "double")
            .add("object", "string")
            .add("offset", "int");
    private static final StructType SILVER_EVENTS_SCHEMA = new StructType()
            .add("event_id", "string", false)
            .add("day", "int")
            .add("month_year", "int")
            .add("year", "int")
            .add("fraction_date", "double")
            .add("date_added", "date")
            .add("partition_date", "date", false)
            .add("batch_time", "string", false)
            .add("source_url", "string");
    private static final StructType SILVER_EVENT_NETWORK_SCHEMA = new StructType()
            .add("event_id", "string", false)
            .add("actor1_code", "string")
            .add("actor1_name", "string")
            .add("actor1_country_code", "string")
            .add("actor1_known_group_code", "string")
            .add("actor1_ethnic_code", "string")
            .add("actor1_religion1_code", "string")
            .add("actor1_religion2_code", "string")
            .add("actor1_type1_code", "string")
            .add("actor1_type2_code", "string")
            .add("actor1_type3_code", "string")
            .add("actor1_geo_type", "int")
            .add("actor1_geo_fullname", "string")
            .add("actor1_geo_country_code", "string")
            .add("actor1_geo_adm1_code", "string")
            .add("actor1_geo_adm2_code", "string")
            .add("actor1_geo_lat", "double")
            .add("actor1_geo_long", "double")
            .add("actor1_geo_feature_id", "string")
            .add("actor2_code", "string")
            .add("actor2_name", "string")
            .add("actor2_country_code", "string")
            .add("actor2_known_group_code", "string")
            .add("actor2_ethnic_code", "string")
            .add("actor2_religion1_code", "string")
            .add("actor2_religion2_code", "string")
            .add("actor2_type1_code", "string")
            .add("actor2_type2_code", "string")
            .add("actor2_type3_code", "string")
            .add("actor2_geo_type", "int")
            .add("actor2_geo_fullname", "string")
            .add("actor2_geo_country_code", "string")
            .add("actor2_geo_adm1_code", "string")
            .add("actor2_geo_adm2_code", "string")
            .add("actor2_geo_lat", "double")
            .add("actor2_geo_long", "double")
            .add("actor2_geo_feature_id", "string")
            .add("is_root_event", "boolean")
            .add("event_code", "string")
            .add("event_base_code", "string")
            .add("event_root_code", "string")
            .add("quad_class", "int")
            .add("goldstein_scale", "double")
            .add("action_geo_type", "int")
            .add("action_geo_fullname", "string")
            .add("action_geo_country_code", "string")
            .add("action_geo_adm1_code", "string")
            .add("action_geo_adm2_code", "string")
            .add("action_geo_lat", "double")
            .add("action_geo_long", "double")
            .add("action_geo_feature_id", "string")
            .add("partition_date", "date", false)
            .add("batch_time", "string", false);
    private static final StructType SILVER_MENTIONS_SCHEMA = new StructType()
            .add("document_id", "string", false)
            .add("event_id", "string", false)
            .add("partition_date", "date", false)
            .add("batch_time", "string", false)
            .add("event_time_date", "date")
            .add("mention_time_date", "date")
            .add("mention_type", "string")
            .add("mention_source_name", "string")
            .add("mention_identifier", "string")
            .add("sentence_id", "int")
            .add("in_raw_text", "boolean")
            .add("confidence", "int")
            .add("mention_doc_len", "int")
            .add("mention_doc_tone", "double")
            .add("extras", "string")
            .add("actor1_char_offset", "int")
            .add("actor2_char_offset", "int")
            .add("action_char_offset", "int");

    public static Map<String, StructType> schemaMap = Map.ofEntries(
            Map.entry("gkg", GKG_SCHEMA),
            Map.entry("events", EVENTS_SCHEMA),
            Map.entry("mentions", MENTIONS_SCHEMA),
            Map.entry("silver_documents", SILVER_DOCUMENTS_SCHEMA),
            Map.entry("silver_gkg", SILVER_GKG_SCHEMA),
            Map.entry("silver_gkg_counts", SILVER_GKG_COUNTS_SCHEMA),
            Map.entry("silver_gkg_locations", SILVER_GKG_LOCATIONS_SCHEMA),
            Map.entry("silver_gkg_themes", SILVER_GKG_THEMES_SCHEMA),
            Map.entry("silver_gkg_persons", SILVER_GKG_PERSONS_SCHEMA),
            Map.entry("silver_gkg_organizations", SILVER_GKG_ORGANIZATIONS_SCHEMA),
            Map.entry("silver_gkg_dates", SILVER_GKG_DATES_SCHEMA),
            Map.entry("silver_gkg_gcam", SILVER_GKG_GCAM_SCHEMA),
            Map.entry("silver_gkg_quotations", SILVER_GKG_QUOTATIONS_SCHEMA),
            Map.entry("silver_gkg_all_names", SILVER_GKG_ALL_NAMES_SCHEMA),
            Map.entry("silver_gkg_amounts", SILVER_GKG_AMOUNTS_SCHEMA),
            Map.entry("silver_events", SILVER_EVENTS_SCHEMA),
            Map.entry("silver_event_network", SILVER_EVENT_NETWORK_SCHEMA),
            Map.entry("silver_mentions", SILVER_MENTIONS_SCHEMA));

    private static final Map<String, String> GKG_REGEX = Map.ofEntries(
            Map.entry("GKGRecordID", "^\\d{14}-T?\\d+$"),
            Map.entry("V2_1Date", "^(?:\\d{14}|0)$"),
            Map.entry("V2SourceCollectionIdentifier", "^\\d+$"),
            Map.entry("V2SourceCommonName", "^[^\\t]*$"),
            Map.entry("V2DocumentIdentifier", "^[^\\t]*$"),
            Map.entry("V1Counts", "^(?:([^#;]*#){9}[^#;]*(?:;([^#;]*#){9}[^#;]*)*+;?)?$"),
            Map.entry("V2_1Counts", "^(?:([^#;]*#){10}[^#;]*(?:;([^#;]*#){10}[^#;]*)*+;?)?$"),
            Map.entry("V1Themes", "^(?:[^;]*(?:;[^;]*)*+)?$"),
            Map.entry("V2EnhancedThemes", "^(?:[^,;]*,[^,;]*(?:;[^,;]*,[^,;]*)*+;?)?$"),
            Map.entry("V1Locations", "^(?:([^#;]*#){6,7}[^#;]*(?:;([^#;]*#){6,7}[^#;]*)*+)?$"),
            Map.entry("V2EnhancedLocations", "^(?:([^#;]*#){8}[^#;]*(?:;([^#;]*#){8}[^#;]*)*+)?$"),
            Map.entry("V1Persons", "^(?:[^;]*(?:;[^;]*)*+)?$"),
            Map.entry("V2EnhancedPersons", "^(?:[^,;]*,[^,;]*(?:;[^,;]*,[^,;]*)*+)?$"),
            Map.entry("V1Organizations", "^(?:[^;]*(?:;[^;]*)*+)?$"),
            Map.entry("V2EnhancedOrganizations", "^(?:[^,;]*,[^,;]*(?:;[^,;]*,[^,;]*)*+)?$"),
            Map.entry("V1_5Tone", "^(?:([^,]*,){6}[^,]*)?$"),
            Map.entry("V2_1EnhancedDates", "^(?:([^#;]*#){4}[^#;]*(?:;([^#;]*#){4}[^#;]*)*+)?$"),
            Map.entry("V2GCAM", "^(?:[^,:]+:[^,]*(?:,[^,:]+:[^,]*)*+)?$"),
            Map.entry("V2_1SharingImage", "^[^\\t]*$"),
            Map.entry("V2_1RelatedImages", "^(?:[^;]*(?:;[^;]*)*)?$"),
            Map.entry("V2_1SocialImageEmbeds", "^(?:[^;]*(?:;[^;]*)*)?$"),
            Map.entry("V2_1SocialVideoEmbeds", "^(?:[^;]*(?:;[^;]*)*)?$"),
            Map.entry("V2_1Quotations", "^[^\\t]*$"),
            Map.entry("V2_1AllNames", "^(?:[^,;]*,[^,;]*(?:;[^,;]*,[^,;]*)*+)?$"),
            Map.entry("V2_1Amounts", "^(?:([^,;]*,){2}[^,;]*(?:;([^,;]*,){2}[^,;]*)*+;?)?$"),
            Map.entry("V2_1TranslationInfo", "^[^\\t]*$"),
            Map.entry("V2ExtraSXML", "^[^\\t]*$"));

    private static final Map<String, String> EVENTS_REGEX = Map.ofEntries(
            Map.entry("GlobalEventID", "^\\d+$"),
            Map.entry("Day", "^\\d{8}$"),
            Map.entry("MonthYear", "^\\d{6}$"),
            Map.entry("Year", "^\\d{4}$"),
            Map.entry("FractionDate", "^\\d{4}(?:\\.\\d+)?$"),
            Map.entry("Actor1Code", "^[A-Za-z0-9]*$"),
            Map.entry("Actor1Name", "^[^\\t]*$"),
            Map.entry("Actor1CountryCode", "^(?:[A-Z0-9]{3})?$"),
            Map.entry("Actor1KnownGroupCode", "^(?:[A-Z0-9]{3})?$"),
            Map.entry("Actor1EthnicCode", "^(?:[A-Za-z0-9]{3})?$"),
            Map.entry("Actor1Religion1Code", "^(?:[A-Z0-9]{3})?$"),
            Map.entry("Actor1Religion2Code", "^(?:[A-Z0-9]{3})?$"),
            Map.entry("Actor1Type1Code", "^(?:[A-Z0-9]{3})?$"),
            Map.entry("Actor1Type2Code", "^(?:[A-Z0-9]{3})?$"),
            Map.entry("Actor1Type3Code", "^(?:[A-Z0-9]{3})?$"),
            Map.entry("Actor2Code", "^[A-Za-z0-9]*$"),
            Map.entry("Actor2Name", "^[^\\t]*$"),
            Map.entry("Actor2CountryCode", "^(?:[A-Z0-9]{3})?$"),
            Map.entry("Actor2KnownGroupCode", "^(?:[A-Z0-9]{3})?$"),
            Map.entry("Actor2EthnicCode", "^(?:[A-Za-z0-9]{3})?$"),
            Map.entry("Actor2Religion1Code", "^(?:[A-Z0-9]{3})?$"),
            Map.entry("Actor2Religion2Code", "^(?:[A-Z0-9]{3})?$"),
            Map.entry("Actor2Type1Code", "^(?:[A-Z0-9]{3})?$"),
            Map.entry("Actor2Type2Code", "^(?:[A-Z0-9]{3})?$"),
            Map.entry("Actor2Type3Code", "^(?:[A-Z0-9]{3})?$"),
            Map.entry("IsRootEvent", "^[01]$"),
            Map.entry("EventCode", "^\\d{2,4}$"),
            Map.entry("EventBaseCode", "^\\d{2,3}$"),
            Map.entry("EventRootCode", "^\\d{2}$"),
            Map.entry("QuadClass", "^[1-4]$"),
            Map.entry("GoldsteinScale", "^-?(?:\\d+(?:\\.\\d*)?|\\.\\d+)$"),
            Map.entry("NumMentions", "^\\d+$"),
            Map.entry("NumSources", "^\\d+$"),
            Map.entry("NumArticles", "^\\d+$"),
            Map.entry("AvgTone", "^-?(?:\\d+(?:\\.\\d*)?|\\.\\d+)$"),
            Map.entry("Actor1GeoType", "^[0-5]$"),
            Map.entry("Actor1GeoFullName", "^[^\\t]*$"),
            Map.entry("Actor1GeoCountryCode", "^(?:[A-Z0-9]{2})?$"),
            Map.entry("Actor1GeoADM1Code", "^[A-Z0-9]*$"),
            Map.entry("Actor1GeoADM2Code", "^[A-Z0-9]*$"),
            Map.entry("Actor1GeoLat", "^(?:-?(?:\\d+(?:\\.\\d*)?|\\.\\d+))?$"),
            Map.entry("Actor1GeoLong", "^(?:-?(?:\\d+(?:\\.\\d*)?|\\.\\d+))?$"),
            Map.entry("Actor1GeoFeatureID", "^[A-Za-z0-9+-]*$"),
            Map.entry("Actor2GeoType", "^[0-5]$"),
            Map.entry("Actor2GeoFullName", "^[^\\t]*$"),
            Map.entry("Actor2GeoCountryCode", "^(?:[A-Z0-9]{2})?$"),
            Map.entry("Actor2GeoADM1Code", "^[A-Z0-9]*$"),
            Map.entry("Actor2GeoADM2Code", "^[A-Z0-9]*$"),
            Map.entry("Actor2GeoLat", "^(?:-?(?:\\d+(?:\\.\\d*)?|\\.\\d+))?$"),
            Map.entry("Actor2GeoLong", "^(?:-?(?:\\d+(?:\\.\\d*)?|\\.\\d+))?$"),
            Map.entry("Actor2GeoFeatureID", "^[A-Za-z0-9+-]*$"),
            Map.entry("ActionGeoType", "^[0-5]$"),
            Map.entry("ActionGeoFullName", "^[^\\t]*$"),
            Map.entry("ActionGeoCountryCode", "^(?:[A-Z0-9]{2})?$"),
            Map.entry("ActionGeoADM1Code", "^[A-Z0-9]*$"),
            Map.entry("ActionGeoADM2Code", "^[A-Z0-9]*$"),
            Map.entry("ActionGeoLat", "^(?:-?(?:\\d+(?:\\.\\d*)?|\\.\\d+))?$"),
            Map.entry("ActionGeoLong", "^(?:-?(?:\\d+(?:\\.\\d*)?|\\.\\d+))?$"),
            Map.entry("ActionGeoFeatureID", "^[A-Za-z0-9+-]*$"),
            Map.entry("DateAdded", "^\\d{14}$"),
            Map.entry("SourceURL", "^[^\\t]*$"));
    private static final Map<String, String> MENTIONS_REGEX = Map.ofEntries(
            Map.entry("GlobalEventID", "^\\d+$"),
            Map.entry("EventTimeDate", "^\\d{14}$"),
            Map.entry("MentionTimeDate", "^\\d{14}$"),
            Map.entry("MentionType", "^[1-6]$"),
            Map.entry("MentionSourceName", "^[^\\t]*$"),
            Map.entry("MentionIdentifier", "^[^\\t]*$"),
            Map.entry("SentenceID", "^\\d+$"),
            Map.entry("Actor1CharOffset", "^-?\\d+$"),
            Map.entry("Actor2CharOffset", "^-?\\d+$"),
            Map.entry("ActionCharOffset", "^-?\\d+$"),
            Map.entry("InRawText", "^[01]$"),
            Map.entry("Confidence", "^(?:100|[1-9]\\d?)$"),
            Map.entry("MentionDocLen", "^\\d+$"),
            Map.entry("MentionDocTone", "^-?(?:\\d+(?:\\.\\d*)?|\\.\\d+)$"),
            Map.entry("MentionDocTranslationInfo", "^(?:|[^;]*;[^;]*)$"),
            Map.entry("Extras", "^[^\\t]*$"));

    public static Map<String, Map<String, String>> regexMap = Map.of(
            "gkg", GKG_REGEX,
            "events", EVENTS_REGEX,
            "mentions", MENTIONS_REGEX);

    static {
        // fail fast if regex are not valid
        regexMap.forEach((tableName, columns) -> columns.forEach((columnName, regex) -> {
            try {
                Pattern.compile(regex);
            } catch (PatternSyntaxException pse) {
                throw new IllegalArgumentException(
                        "Invalid regex for " + tableName + "." + columnName,
                        pse);
            }
        }));
    }

    private Schema() {
    }
}
