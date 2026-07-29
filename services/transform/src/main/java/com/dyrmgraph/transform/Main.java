package com.dyrmgraph.transform;

import java.sql.*;
import java.time.LocalDate;
import java.util.Set;
import java.util.Map;

public final class Main {

    private Main() {
    }

    public static void main(String[] args) throws SQLException {

        // Probably not needed. Inputs are read from PG
        // if (args.length != 2) {
        // System.err.println(
        // "Usage: DummyTransformJob <input-path> <output-path>");
        // System.exit(2);
        // }

        try (Connection conn = DyrmgraphConnection.getPGConn()) {
            Set<LocalDate> result = QueryExecutor.getPendingJobs(conn);
            Map<LocalDate, Map<String, Helpers.Paths>> paths = Helpers.buildPaths(result, conn);
            TransformUtil.run(paths);
        }
    }

}