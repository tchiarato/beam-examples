package com.chiarato.beam.examples.utils;

import java.util.concurrent.ThreadLocalRandom;

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

  private static final Duration FIVE_MINUTES = Duration.standardMinutes(5);
  private static final Duration TEN_MINUTES = Duration.standardMinutes(10);

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
            .withIdAttribute("event_uuid")
            .fromTopic(this.topicName));
      }

      return events;
    }

    private TestStream<String> mockEvents() {
      return TestStream.create(StringUtf8Coder.of())
        .advanceWatermarkTo(baseTime)
        .addElements(
          event("TYPE-01", Duration.ZERO),
          event("TYPE-01", Duration.standardSeconds(30)),
          event("TYPE-02", Duration.standardSeconds(31)),
          event("TYPE-02", Duration.standardSeconds(32)),
          event("TYPE-02", Duration.standardSeconds(50)))
        .advanceProcessingTime(FIVE_MINUTES)
        .addElements(
          event("TYPE-01", Duration.standardMinutes(1)))
        .advanceWatermarkTo(baseTime.plus(TEN_MINUTES))
        .addElements(
          event("TYPE-01", Duration.standardMinutes(6)),
          event("TYPE-03", Duration.standardMinutes(9)))
        .advanceProcessingTime(TEN_MINUTES.plus(FIVE_MINUTES))
        .advanceWatermarkTo(baseTime.plus(TEN_MINUTES.plus(FIVE_MINUTES)))
        .addElements(
          event("TYPE-04", Duration.standardMinutes(9)))
        .advanceWatermarkToInfinity();
    }

    private TimestampedValue<String> event(String eventType, Duration baseTimeOffset) {
      int age = ThreadLocalRandom.current().nextInt(30, 40 + 1);
      return TimestampedValue.of(  
        "{ \"event_type\": \"" + eventType + "\", \"event_timestamp\": \"" + baseTime.plus(baseTimeOffset).toDateTime().toString() + "\", \"payload\": { \"email\": \"chiarato@gmail.com\", \"age\": \"" + age + "\", \"pet\": { \"name\": \"Little Ball\" } } }",
        Instant.now());
    }
  }
}