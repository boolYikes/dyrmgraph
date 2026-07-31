package com.dyrmgraph.transform;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.List;

import java.sql.SQLException;
import java.sql.Connection;
import java.time.LocalDate;

import java.nio.file.Path;
import java.io.IOException;
import java.nio.file.Files;

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
            int newVersion = QueryExecutor.getRevision(date, conn) + 1;
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

    static void writeXCOM(String pathString, Map<String, Object> payload) throws IOException {
        Path path = Path.of(pathString);
        Files.createDirectories(path.getParent());
        ObjectMapper mapper = new ObjectMapper();
        mapper.writeValue(path.toFile(), payload);
    }

}
