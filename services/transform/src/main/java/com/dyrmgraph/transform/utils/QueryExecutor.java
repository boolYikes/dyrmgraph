package com.dyrmgraph.transform.utils;

import java.sql.ResultSet;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

public final class QueryExecutor {
    private QueryExecutor() {
    }

    // record PendingJobs(LocalDate partitionDate, List<String> objectPaths) {}

    public static List<LocalDateTime> getPendingJobs(Connection conn) throws SQLException {
        // The justification for keeping the transform_runs table on top of the csv file
        // manifest
        // is to track the runs, report and for notification generation.
        try (
                Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery("""
                            WITH running AS (
                                UPDATE transform_runs
                                SET status = 'running'
                                WHERE status = 'claimed'
                                RETURNING *
                            )
                            SELECT file_datetime
                            FROM running
                        """);) {
            List<LocalDateTime> result = new ArrayList<>();

            while (rs.next()) {
                LocalDateTime dt = rs.getObject("file_datetime", LocalDateTime.class);
                result.add(dt);
            }
            return result;
        }
    }

    // TODO: This is a placeholder/template
    public static Map<LocalDate, Integer> getPendingCompactionJobs(Connection conn) throws SQLException {
        try (
                Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery("""
                                WITH running AS (
                                    UPDATE compaction_runs
                                    SET status = 'running'
                                    WHERE status = 'claimed'
                                    RETURNING *
                                )
                                SELECT target_date, MAX(version) as version
                                FROM running
                                GROUP BY target_date
                        """)) {

            Map<LocalDate, Integer> result = new HashMap<>();

            while (rs.next()) {
                LocalDate partitionDate = rs.getObject("target_date", LocalDate.class);
                int version = rs.getObject("version", Integer.class);

                // upstream is already atomic on the three tables (fails if all three tables
                // werr not downloaded successfully)
                // so, I don't need the info as to which tables I have to transform.
                result.put(partitionDate, version);
            }

            return result;
        } // this closes rs and st automatically
    }

    /**
     * Obsolete: This is done from getPendingJobs now.
     */
    public static int getRevision(LocalDate date, Connection conn) throws SQLException {
        String sql = "SELECT MAX (version) AS version " +
                "FROM transform_runs " +
                "WHERE partition_date = ?";
        // reminder: all three tables are atomic!!!
        // same date, max revision number
        int result = 0;
        try (
                PreparedStatement st = conn.prepareStatement(sql)) {
            st.setObject(1, date);
            try (ResultSet rs = st.executeQuery()) {
                if (rs.next()) {
                    result = rs.getObject("version", Integer.class);
                }
            }
        }
        return result;
    }
}
