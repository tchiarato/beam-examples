package com.chiarato.beam.examples;

import java.util.Arrays;
import java.util.List;

import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
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

  static final String[] EVENTS_ARRAY =
    new String[] {
      "{ 'event_type': 'TYPE-01' }",
      "{ 'event_type': 'TYPE-02' }",
      "{ 'event_type': 'TYPE-01' }",
      "{ 'event_type': 'TYPE-01' }",
      "{ 'event_type': 'TYPE-03' }"
    };

  static final List<String> EVENTS = Arrays.asList(EVENTS_ARRAY);
  static final String[] COUNTS_ARRAY = new String[] { "TYPE-01: 3", "TYPE-02: 1", "TYPE-03: 1" };

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  @Category(ValidatesRunner.class)
  public void testExtractAndSumEventTypes() {
    PCollection<String> input = 
      pipeline.apply(Create.of(EVENTS).withCoder(StringUtf8Coder.of()));
    
    PCollection<String> output = 
      input
        .apply(new Example02.ExtractAndSumEventTypes())
        .apply(MapElements.via(new Example02.FormatAsTextFn()));
    
      PAssert.that(output).containsInAnyOrder(COUNTS_ARRAY);

      pipeline.run().waitUntilFinish();
  }
}