package com.chiarato.beam.examples;

import com.chiarato.beam.examples.utils.ExerciseOptions;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * Example 01 - Reads events from a Pubsub topic and writes to a specific directory.
 * To run this code:
 *
 * mvn compile exec:java \
 *  -Dexec.mainClass="br.com.resultadosdigitais.examples.Example02" \
 *  -Dexec.args="--runner=DirectRunner \
 *      --outputPrefix=output/events \
 *      --topicName=projects/project-name/topics/topic-name"
 */
public class Example02 {
  public static void main(String[] args) {
    ExerciseOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(ExerciseOptions.class);

    Pipeline pipeline = Pipeline.create(options);

    TestStream<String> events = TestStream.create(StringUtf8Coder.of())
      .addElements("1.foo", "2.bar")
      .advanceWatermarkTo(Instant.now())
      .advanceWatermarkToInfinity();

    // pipeline
    //   .apply("Read Messages from Pubsub",
    //     PubsubIO
    //       .readStrings()
    //       .fromTopic(options.getTopicName()))

    pipeline
      .apply(events)
      .apply("Set event timestamp", ParDo.of(new DoFn<String, String>() {
        @ProcessElement
        public void processElement(ProcessContext context) {
          context.outputWithTimestamp(context.element(), Instant.now());
        }
      }))

      .apply("Windowing", Window.into(FixedWindows.of(Duration.standardMinutes(5))))

      .apply("Write to File",
        TextIO
          .write()
          .withWindowedWrites()
          .withNumShards(1)
          .to(options.getOutputPrefix()));

    pipeline.run();
  }
}