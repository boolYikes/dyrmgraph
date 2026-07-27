package com.dyrmgraph.transform;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DummyTransformJobTest {

    private static SparkSession spark;

    @BeforeAll
    static void setUpSpark() {
        spark = SparkSession.builder()
                .master("local[2]")
                .appName("transform-test")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.shuffle.partitions", "2")
                .getOrCreate();
    }

    @AfterAll
    static void tearDownSpark() {
        if (spark != null) {
            spark.stop();
        }
    }

    @Test
    void removesNullEventIdsAndTrimsRemainingIds() {
        Dataset<Row> input = spark.createDataFrame(
                List.of(
                        new Event("  event-1  ", "foo"),
                        new Event(null, "bar"),
                        new Event("event-3", "baz")),
                Event.class);

        Dataset<Row> result = DummyTransformJob.transform(input);

        List<String> eventIds = result
                .select("eventId")
                .as(
                        org.apache.spark.sql.Encoders.STRING())
                .collectAsList();

        assertEquals(
                List.of("event-1", "event-3"),
                eventIds);
    }

    public static class Event {

        private String eventId;
        private String payload;

        public Event() {
        }

        public Event(String eventId, String payload) {
            this.eventId = eventId;
            this.payload = payload;
        }

        public String getEventId() {
            return eventId;
        }

        public void setEventId(String eventId) {
            this.eventId = eventId;
        }

        public String getPayload() {
            return payload;
        }

        public void setPayload(String payload) {
            this.payload = payload;
        }
    }
}
