package com.dyrmgraph.transform;

import java.sql.ResultSet;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.SQLException;

import java.time.LocalDate;
import java.util.Set;
import java.util.HashSet;

public final class QueryExecutor {
    private QueryExecutor() {
    }

    // record PendingJobs(LocalDate partitionDate, List<String> objectPaths) {}

    static Set<LocalDate> getPendingJobs(Connection conn) throws SQLException {
        try (
                Statement st = conn.createStatement();
                ResultSet rs = st
                        .executeQuery("SELECT DISTINCT partition_date FROM transform_runs WHERE status = 'running'");) {

            Set<LocalDate> result = new HashSet<>();

            while (rs.next()) {
                LocalDate partitionDate = rs.getObject("partition_date", LocalDate.class);

                // upstream is already atomic on the three tables (fails if all three tables
                // werr not downloaded successfully)
                // so, I don't need the info as to which tables I have to transform.
                result.add(partitionDate);
            }

            return result;
            // this closes rs and st automatically
        }
    }

    static String getRevision(LocalDate date, Connection conn) throws SQLException {
        String newVersion = "5";
        return newVersion;
    }
}
