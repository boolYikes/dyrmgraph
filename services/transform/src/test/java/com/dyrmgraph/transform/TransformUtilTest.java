package com.dyrmgraph.transform;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.List;
import java.nio.file.Files;
import java.nio.file.Path;
import java.io.File;
import java.io.IOException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import org.mockito.MockedStatic;
import org.mockito.MockedConstruction;
import static org.mockito.Mockito.*;

import com.dyrmgraph.transform.utils.Helpers;
import com.fasterxml.jackson.databind.ObjectMapper;

class TransformUtilTest {
    /**
     * TransformUtil.validate() test
     */
    @Test
    void testValidate() {
        Dataset<?> mockDF = mock(Dataset.class);
        String mockTableName = "mock_table";
    }
}
