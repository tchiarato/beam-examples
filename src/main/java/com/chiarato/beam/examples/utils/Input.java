package com.chiarato.beam.examples.utils;

import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * Input
 */
public class Input {

  public static class BoundedData extends PTransform<PBegin, PCollection<String>> {

    private String fileName;

    public BoundedData(String fileName) {
      this.fileName = fileName;
    }

    @Override
    public PCollection<String> expand(PBegin input) {
      return input.apply(
        TextIO
          .read()
          .from(fileName));
    }
  }

  /**
   * UnboundedData
   */
  public static class UnboundedData extends PTransform<PBegin, PCollection<String>> {

    private Instant baseTime = new Instant(0);
    private String runner;
    private String topicName;

    public UnboundedData(String runner, String topicName) {
      this.runner = runner;
      this.topicName = topicName;
    }

    @Override
    public PCollection<String> expand(PBegin input) {
      PCollection<String> events;

      if (this.runner.equals("DirectRunner")) {
        events = input.apply("Mock Data from TestStream", mockEvents());
      } else {
        events = input.apply("Read Messages from Pubsub",
          PubsubIO
            .readStrings()
            .fromTopic(this.topicName));
      }

      return events;
    }

    private TestStream<String> mockEvents() {
      return TestStream.create(StringUtf8Coder.of())
        .advanceWatermarkTo(baseTime)
        .addElements(
          event("CONVERSION", Duration.ZERO),
          event("CONVERSION", Duration.ZERO),
          event("EMAIL_RECEIVED", Duration.standardMinutes(5)))
        .advanceWatermarkTo(baseTime.plus(Duration.standardMinutes(5)))
        .addElements(
          event("CONVERSION", Duration.standardMinutes(5)),
          event("EMAIL_RECEIVED", Duration.standardMinutes(8)))
        .advanceWatermarkTo(baseTime.plus(Duration.standardMinutes(10)))
        .addElements(
          event("CONVERSION", Duration.standardSeconds(1)),
          event("EMAIL_RECEIVED", Duration.standardMinutes(13)))
        .advanceWatermarkToInfinity();
    }

    private TimestampedValue<String> event(String eventType, Duration baseTimeOffset) {
      return TimestampedValue.of(
        "{ 'event_type': '" + eventType + "', 'event_timestamp': '" + baseTime.plus(baseTimeOffset).toDateTime().toString() + "' }",
        baseTime.plus(baseTimeOffset));
    }
  }
}