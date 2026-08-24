package com.dyrmgraph.transform;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import com.dyrmgraph.transform.utils.DyrmgraphConnection;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.times;

import org.apache.spark.sql.SparkSession;

public final class DyrmgraphConnectionTest {
    private DyrmgraphConnectionTest() {
    }

    @Test
    void getSparkSessionReturnsCorrectSession() {
        SparkSession mockSession = mock(SparkSession.class);
        SparkSession.Builder mockBuilder = mock(SparkSession.Builder.class);

        try (MockedStatic<SparkSession> mocked = mockStatic(SparkSession.class)) {
            mocked.when(SparkSession::builder).thenReturn(mockBuilder);

            when(mockBuilder.config("spark.sql.ansi.enabled", "true"))
                    .thenReturn(mockBuilder);
            when(mockBuilder.appName("transform-gdelt-csv"))
                    .thenReturn(mockBuilder);
            when(mockBuilder.getOrCreate())
                    .thenReturn(mockSession);

            SparkSession result = DyrmgraphConnection.getSparkSession();

            assertSame(mockSession, result);

            mocked.verify(SparkSession::builder, times(1));
            verify(mockBuilder).appName("transform-gdelt-csv");
            verify(mockBuilder).getOrCreate();
        }
    }
}