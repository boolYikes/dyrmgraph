package com.dyrmgraph.transform;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.List;
import java.nio.file.Files;
import java.nio.file.Path;
import java.io.File;
import java.io.IOException;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import org.mockito.MockedStatic;
import org.mockito.MockedConstruction;
import static org.mockito.Mockito.*;

import com.dyrmgraph.transform.utils.Helpers;
import com.fasterxml.jackson.databind.ObjectMapper;

class HelpersTest {
    @Test
    void buildPathsReturnsPaths() throws IllegalStateException {
        LocalDateTime dt = LocalDateTime.of(2026, 7, 31, 12, 12, 12);
        List<LocalDateTime> pendingDateTimes = List.of(dt);

        Map<LocalDateTime, Map<String, Helpers.Paths>> expected = Map.of(
                dt, Map.of(
                        "gkg", new Helpers.Paths(
                                "s3a://xx/bronze/gkg/date=2026-07-31/121212.csv",
                                "s3a://xx/silver/%s/",
                                "s3a://xx/silver/%s/"),
                        "mentions", new Helpers.Paths(
                                "s3a://xx/bronze/mentions/date=2026-07-31/121212.csv",
                                "s3a://xx/silver/%s/",
                                "s3a://xx/silver/%s/"),
                        "events", new Helpers.Paths(
                                "s3a://xx/bronze/events/date=2026-07-31/121212.csv",
                                "s3a://xx/silver/%s/",
                                "s3a://xx/silver/%s/")));

        Map<LocalDateTime, Map<String, Helpers.Paths>> result = Helpers.buildPaths(pendingDateTimes, "xx");

        assertEquals(expected, result);
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
