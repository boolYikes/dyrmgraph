package com.dyrmgraph.transform.utils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class GraphUtil {
    private GraphUtil() {

    }

    // NOTE: analytical queries
    public static Dataset<Row> parquetToGraph(Dataset<Row> input) {

        return input;
    }
}
