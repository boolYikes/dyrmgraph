package com.dyrmgraph.transform;

import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.List;

import java.sql.SQLException;
import java.sql.Connection;
import java.time.LocalDate;

public final class Helpers {
    private Helpers() {
    }

    record Paths(String input, String output) {
    }

    // Conceptually...
    // {
    // "date": {
    // "gkg": {
    // "input": "path/to/csv",
    // "output": "table/path/to/part",
    // },
    // "mentions": {
    // "input": "path/to/csv",
    // "output": "table/path/to/partition"
    // }
    // }
    // }
    static Map<LocalDate, Map<String, Paths>> buildPaths(Set<LocalDate> pendingDates, Connection conn)
            throws SQLException {
        String bucket = "xx"; // TODO: will get from env
        String inputStage = "bronze";
        String resultStage = "silver";

        List<String> tables = List.of("gkg", "mentions", "events");

        Map<LocalDate, Map<String, Paths>> result = new HashMap<>();
        for (LocalDate date : pendingDates) {
            String newVersion = QueryExecutor.getRevision(date, conn);
            Map<String, Paths> tablePaths = new HashMap<>();

            for (String table : tables) {
                String inputPath = String.format(
                        "s3a://%s/%s/table=%s/partition_date=%s/",
                        bucket, inputStage, table, date);

                String outputPath = String.format(
                        "s3a://%s/%s/table=%s/partition_date=%s/version=%s/",
                        bucket, resultStage, table, date, newVersion);

                tablePaths.put(table, new Paths(inputPath, outputPath));
            }
            result.put(date, tablePaths);
        }
        return result;
    }
}
