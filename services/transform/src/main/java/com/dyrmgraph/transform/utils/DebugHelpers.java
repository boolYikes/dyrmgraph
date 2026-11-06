package com.dyrmgraph.transform.utils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.array_contains;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.desc;

public final class DebugHelpers {
    public static void debugRegex(Dataset<Row> invalidDF) {
        String[] suspectColumns = {
                "Actor1GeoType",
                "Actor2GeoType",
                "ActionGeoType",
                "Actor1Code",
                "Actor2Code",
                "Actor1EthnicCode",
                "Actor2EthnicCode"
        };

        for (String field : suspectColumns) {
            System.out.println("Invalid values for: " + field);

            invalidDF
                    .filter(array_contains(col("_validation_errors"), field))
                    .groupBy(col(field))
                    .count()
                    .orderBy(desc("count"))
                    .show(30, false);
        }
    }
}
