package com.chiarato.beam.examples.utils;

import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
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

    private String runner;
    private String topicName;

    public UnboundedData(String runner, String topicName) {
      this.runner = runner;
      this.topicName = topicName;
    }

    @Override
    public PCollection<String> expand(PBegin input) {
      PCollection<String> events;

      if (this.runner == "DirectRunner") {
        TestStream<String> mock = TestStream.create(StringUtf8Coder.of())
          .addElements(
            "{ event_timestamp: '2018-09-20T23:10:39Z' }",
            "{ event_timestamp: '2018-09-20T23:10:39Z' }")
          .advanceWatermarkTo(Instant.now())
          .advanceWatermarkToInfinity();

        events = input.apply("Mock Data from TestStream", mock);
      } else {
        events = input.apply("Read Messages from Pubsub",
          PubsubIO
            .readStrings()
            .fromTopic(this.topicName));
      }

      return events;
    }
  }
}