package com.dyrmgraph.transform;

import java.time.LocalDate;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Set;
import java.util.Map;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import org.mockito.MockedStatic;

import static org.mockito.Mockito.*;

class HelpersTest {
        @Test
        void buildPathsReturnsPaths() throws SQLException {
                Connection mockConn = mock(Connection.class);
                LocalDate date = LocalDate.of(2026, 7, 31);
                Set<LocalDate> pendingDates = Set.of(date);

                Map<LocalDate, Map<String, Helpers.Paths>> expected = Map.of(
                                date, Map.of(
                                                "gkg", new Helpers.Paths(
                                                                "s3a://xx/bronze/table=gkg/partition_date=2026-07-31/",
                                                                "s3a://xx/silver/table=gkg/partition_date=2026-07-31/version=6/"),
                                                "mentions", new Helpers.Paths(
                                                                "s3a://xx/bronze/table=mentions/partition_date=2026-07-31/",
                                                                "s3a://xx/silver/table=mentions/partition_date=2026-07-31/version=6/"),
                                                "events", new Helpers.Paths(
                                                                "s3a://xx/bronze/table=events/partition_date=2026-07-31/",
                                                                "s3a://xx/silver/table=events/partition_date=2026-07-31/version=6/")));

                try (MockedStatic<QueryExecutor> queryExecutor = mockStatic(QueryExecutor.class)) {
                        queryExecutor.when(
                                        () -> QueryExecutor.getRevision(date, mockConn))
                                        .thenReturn(5);

                        Map<LocalDate, Map<String, Helpers.Paths>> result = Helpers.buildPaths(pendingDates, mockConn);

                        assertEquals(expected, result);
                }
        }

        @Test
        void buildPathsPropagatesRevisionSQLException() throws SQLException {
                Connection mockConn = mock(Connection.class);
                LocalDate date = LocalDate.of(2026, 7, 31);

                try (MockedStatic<QueryExecutor> queryExecutor = mockStatic(QueryExecutor.class)) {
                        queryExecutor.when(
                                        () -> QueryExecutor.getRevision(date, mockConn))
                                        .thenThrow(new SQLException("Mock SQL Exception"));

                        assertThrows(
                                        SQLException.class,
                                        () -> Helpers.buildPaths(Set.of(date), mockConn));
                }
        }
}
