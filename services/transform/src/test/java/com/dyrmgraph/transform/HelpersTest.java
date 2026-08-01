package com.dyrmgraph.transform;

import java.time.LocalDate;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Set;
import java.util.Map;
import java.nio.file.Files;
import java.nio.file.Path;
import java.io.File;
import java.io.IOException;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import org.mockito.MockedStatic;
import org.mockito.MockedConstruction;
import static org.mockito.Mockito.*;
import com.fasterxml.jackson.databind.ObjectMapper;

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

    @Test
    void writeXCOMDoesItsThing() throws IOException {
        String mockPathString = "test_xcom.json";
        Map<String, Object> mockPayload = Map.of(
                "status", "is_failed",
                "reason", "Test message");

        Path mockPathInstance = mock(Path.class);
        Path mockParentPath = mock(Path.class);

        File mockFile = mock(File.class);
        when(mockPathInstance.getParent()).thenReturn(mockParentPath);
        when(mockPathInstance.toFile()).thenReturn(mockFile);

        try (MockedConstruction<ObjectMapper> mockMapper = mockConstruction(ObjectMapper.class)) {
            try (MockedStatic<Path> mockPath = mockStatic(Path.class)) {
                mockPath.when(() -> Path.of(mockPathString))
                        .thenReturn(mockPathInstance);

                try (MockedStatic<Files> mockFiles = mockStatic(Files.class)) {
                    Helpers.writeXCOM(mockPathString, mockPayload);

                    ObjectMapper mockMapperInstance = mockMapper.constructed().get(0);
                    // i think this line is ineffective?
                    // doNothing().when(mockMapperInstance).writeValue(mockFile, mockPayload);
                    mockFiles.verify(() -> Files.createDirectories(mockParentPath));
                    verify(mockMapperInstance, times(1)).writeValue(mockFile,
                            mockPayload);
                }
            }
        }
    }
}
