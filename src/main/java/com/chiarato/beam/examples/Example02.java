package com.chiarato.beam.examples;

import com.chiarato.beam.examples.utils.ExerciseOptions;
import com.chiarato.beam.examples.utils.Input;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
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
      .apply("Read Unbounded Input Data", new Input.UnboundedData(runner, topic))
      .apply("Windowing Event Types", new WindowedEventTypes())
      .apply("Write Data to File",
        TextIO
          .write()
          .withWindowedWrites()
          .withNumShards(1)
          .to(options.getOutputPrefix()));

    pipeline.run();
  }

  /**
   * Windowing unbounded data by Event Type in windows of 5 minutes.
   */
  public static class WindowedEventTypes extends PTransform<PCollection<String>, PCollection<String>> {

    @Override
    public PCollection<String> expand(PCollection<String> input) {
      return input.
        apply("Extract timestamp from event", ParDo.of(new DoFn<String, String>() {

          @ProcessElement
          public void processElement(ProcessContext context) {
            String message = context.element();

            JSONObject json = new JSONObject(message);
            Instant timestamp = Instant.parse(json.getString("event_timestamp"));

            context.outputWithTimestamp(message, timestamp);
          }
        }))

        .apply("Windowing", Window.into(FixedWindows.of(Duration.standardMinutes(5))))

        .apply("Extract and Sum Event Types", new ExtractAndSumEventTypes())

        .apply("Format as Text", MapElements.via(new FormatAsTextFn()));
    }
  }

  /**
   * Create KV pair of Event Types and their respective quantities;
   */
  public static class ExtractAndSumEventTypes
    extends PTransform<PCollection<String>, PCollection<KV<String, Integer>>> {

    public ExtractAndSumEventTypes() { }

    @Override
    public PCollection<KV<String, Integer>> expand(PCollection<String> input) {
      return input
        .apply("Extract EventTypes", ParDo.of(new DoFn<String, KV<String, Integer>>() {

          @ProcessElement
          public void processElement(ProcessContext context) {
            String message = context.element();

            JSONObject json = new JSONObject(message);
            context.output(KV.of(json.getString("event_type"), 1));
          }
        }))
        .apply("Sum EventTypes", Sum.<String>integersPerKey());
    }
  }

  public static class FormatAsTextFn extends SimpleFunction<KV<String, Integer>, String> {
    @Override
    public String apply(KV<String, Integer> input) {
      return input.getKey() + ": " + input.getValue();
    }
  }
}