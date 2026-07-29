package com.dyrmgraph.transform;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Connection;

import java.util.Map;

import org.apache.spark.sql.SparkSession;

public final class DyrmgraphConnection {

    private DyrmgraphConnection() {
    }

    /**
     * Only configure for application specific things
     * 
     * @return SparkSession
     */
    static SparkSession getSparkSession() {
        return SparkSession.builder()
                .appName("transform-gdelt-csv")
                .getOrCreate();
    }

    /**
     * Use with a try block
     * 
     * @return java.sql.Connection
     * @throws SQLException
     */
    static Connection getPGConn() throws SQLException {
        Map<String, String> env = System.getenv();
        String user = env.get("MANIFEST_PG_USER");
        String pw = env.get("MANIFEST_PG_PASSWORD");
        String db = env.get("MANIFEST_PG_DB");
        String host = env.get("MANIFEST_PG_HOST");
        String port = env.get("MANIFEST_PG_PORT");

        String url = String.format("jdbc:postgresql://%s:%s/%s", host, port, db);

        Connection conn = DriverManager.getConnection(url, user, pw);
        return conn;
    }
}
