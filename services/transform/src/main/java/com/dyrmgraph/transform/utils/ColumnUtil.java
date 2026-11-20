package com.dyrmgraph.transform.utils;

import org.apache.spark.sql.Column;
import static org.apache.spark.sql.functions.*;

public final class ColumnUtil {
    private ColumnUtil() {
    }

    public static Column safeGet(Column array, int index) {
        return get(array, lit(index));
    }

    public static void buildSimpleDerivedColumn() {

    }

    public static Column buildComputedColumn(
            Column record,
            boolean isCount) {
        int featureIdIndex = isCount ? 9 : 7;
        Column featureId = safeGet(record, featureIdIndex);
        Column[] columns = isCount
                ? new Column[] {
                        lit("geo"),
                        coalesce(safeGet(record, 3), lit("NULL")),
                        coalesce(safeGet(record, 4), lit("NULL")),
                        coalesce(safeGet(record, 5), lit("NULL")),
                        coalesce(safeGet(record, 6), lit("NULL")),
                        coalesce(safeGet(record, 7), lit("NULL")),
                        coalesce(safeGet(record, 8), lit("NULL"))
                }
                : new Column[] {
                        lit("geo"),
                        coalesce(record.getItem(0), lit("NULL")),
                        coalesce(record.getItem(1), lit("NULL")),
                        coalesce(record.getItem(2), lit("NULL")),
                        coalesce(record.getItem(3), lit("NULL")),
                        // Count doesn't use ADM2. Don't use it in location either
                        coalesce(record.getItem(5), lit("NULL")),
                        coalesce(record.getItem(6), lit("NULL"))
                };
        return when(
                featureId.isNotNull().and(not(featureId.equalTo(""))),
                concat(lit("fid:"), featureId))
                .otherwise(concat_ws(":", columns))
                .alias("location_computed_id");
    }

    public static void buildExplodedColumn() {

    }
}
