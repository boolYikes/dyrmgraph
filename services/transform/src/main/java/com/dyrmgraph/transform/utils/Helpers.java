package com.dyrmgraph.transform.utils;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Map;
import java.util.HashMap;
import java.util.List;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.nio.file.Path;
import java.nio.file.Files;
import java.io.IOException;

public final class Helpers {
    private Helpers() {
    }

    public record Paths(
            String inputPath,
            String outputPath,
            String errorPath) {
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
    public static Map<LocalDateTime, Map<String, Paths>> buildPaths(List<LocalDateTime> pendingDateTimes,
            String bucket) {

        String inputStage = "bronze";
        String resultStage = "silver";

        List<String> tables = List.of("gkg", "mentions", "events");

        Map<LocalDateTime, Map<String, Paths>> result = new HashMap<>();
        for (LocalDateTime dt : pendingDateTimes) {
            Map<String, Paths> tablePaths = new HashMap<>();

            for (String table : tables) {
                String inputPath = String.format(
                        "s3a://%s/%s/%s/date=%s/%s.csv",
                        bucket, inputStage, table, dt.toLocalDate(),
                        dt.toLocalTime().format(DateTimeFormatter.ofPattern("HHmmss")));

                // table name is deferred using escape so that it can be done dynamically later
                String outputPath = String.format(
                        "s3a://%s/%s/%%s/",
                        bucket, resultStage);

                String errorPath = String.format(
                        "s3a://%s/%s/%%s/",
                        bucket, resultStage);

                tablePaths.put(table, new Paths(inputPath, outputPath, errorPath));
            }
            result.put(dt, tablePaths);
        }
        return result;
    }

    public static void writeXCOM(String pathString, Map<String, Object> payload) throws IOException {
        Path path = Path.of(pathString);
        Files.createDirectories(path.getParent());
        ObjectMapper mapper = new ObjectMapper();
        mapper.writeValue(path.toFile(), payload);
    }

}
