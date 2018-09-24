package com.chiarato.beam.examples;

import com.chiarato.beam.examples.utils.ExerciseOptions;
import com.chiarato.beam.examples.utils.Input;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.json.JSONObject;

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

    String runner = options.getRunner().getSimpleName();
    String topic = options.getTopicName();

    pipeline
      .apply(new Input.UnboundedData(runner, topic))
      .apply("Extract timestamp from event", ParDo.of(new DoFn<String, String>() {

        @ProcessElement
        public void processElement(ProcessContext context) {
          String message = context.element();

          JSONObject json = new JSONObject(message);
          Instant timestamp = Instant.parse(json.getString("event_timestamp"));

          context.outputWithTimestamp(message, timestamp);
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