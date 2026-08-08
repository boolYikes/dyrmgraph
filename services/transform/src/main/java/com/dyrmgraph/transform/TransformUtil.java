// NOTE: should I use instance-oriented methods?
package com.dyrmgraph.transform;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Map;
import java.util.function.Function;

public final class TransformUtil {

    private static Map<String, Map<String, Function<Dataset<Row>, Dataset<Row>>>> yup = Map.of(
            "gkg", Map.of("validate", TransformUtil::validateGKG, "normalize", TransformUtil::normalizeGKG),
            "events", Map.of("validate", TransformUtil::validateEvents, "normalize", TransformUtil::normalizeEvents),
            "mentions",
            Map.of("validate", TransformUtil::validateMentions, "normalize", TransformUtil::normalizeMentions));

    private TransformUtil() {
    }

    private static Dataset<Row> validateGKG(Dataset<Row> input) {
        return input;
    }

    private static Dataset<Row> validateEvents(Dataset<Row> input) {
        return input;
    }

    private static Dataset<Row> validateMentions(Dataset<Row> input) {
        return input;
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

    static Dataset<Row> validateSchema(Dataset<Row> input, String tableName) {
        // parse cols and data types (date conversion, nulls, malformed etc)
        Dataset<Row> valid = yup.get(tableName).get("validate").apply(input);
        return valid;
    }

    static Map<String, Dataset<Row>> normalizeTables(Dataset<Row> input, String tableName) {
        // 4.2 dedupe with unique ids
        // 4.3 explode nested cols

        // TODO: return products from the parent table
        Dataset<Row> clean = yup.get(tableName).get("normalize").apply(input);
        return Map.of(tableName, clean);
    }

}
