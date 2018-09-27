package com.chiarato.beam.examples;

import com.chiarato.beam.examples.utils.Input;

import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Example02Test
 */
@RunWith(JUnit4.class)
public class Example02Test {

  static final Duration FIVE_MINUTES = Duration.standardMinutes(5);
  static final Duration TEN_MINUTES = Duration.standardMinutes(10);

  static final String[] COUNTS_ARRAY = new String[] { "TYPE-01: 3", "TYPE-02: 1", "TYPE-03: 1" };
  static final Duration EVENT_WINDOW_DURATION = Duration.standardMinutes(10);
  static final Instant baseTime = new Instant(0);

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  @Category(ValidatesRunner.class)
  public void testExtractAndSumEventTypes() {
    BoundedWindow window = new IntervalWindow(baseTime, EVENT_WINDOW_DURATION);

    PCollection<String> output =
      pipeline
        .apply(new Input.UnboundedData("DirectRunner", null))
        .apply(new Example02.WindowedEventTypes());

      PAssert
        .that(output)
        .inOnTimePane(window)
        .containsInAnyOrder(new String[] { "TYPE-02: 1",  "TYPE-01: 2" });
      
      PAssert
        .that(output)
        .inWindow(window)
        .containsInAnyOrder(new String[] { "TYPE-02: 1",  "TYPE-01: 2", "TYPE-01: 1", "TYPE-03: 1" });

      PAssert
        .that(output)
        .inFinalPane(window)
        .containsInAnyOrder(new String[] { "TYPE-04: 1" });

      pipeline.run().waitUntilFinish();
  }

  private TimestampedValue<String> event(String eventType, Duration baseTimeOffset) {
    return TimestampedValue.of(
      "{ 'event_type': '" + eventType + "', 'event_timestamp': '" + baseTime.plus(baseTimeOffset).toDateTime().toString() + "' }",
      baseTime.plus(baseTimeOffset));
  }
}