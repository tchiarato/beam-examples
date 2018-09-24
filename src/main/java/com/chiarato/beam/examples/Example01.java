package com.chiarato.beam.examples;

import com.chiarato.beam.examples.utils.ExerciseOptions;
import com.chiarato.beam.examples.utils.Input;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.joda.time.Duration;

/**
 * Example 01 - Read events from a text file and writes to a specific directory.
 * To run this code:
 *
 * mvn compile exec:java -Dexec.mainClass="br.com.resultadosdigitais.examples.Example01" -Dexec.args="--runner=DirectRunner --outputPrefix=output/raw/"
 */
public class Example01 {
  private static String INPUT = "input/sample.txt";

  public static void main(String[] args) {
    ExerciseOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(ExerciseOptions.class);
    Pipeline pipeline = Pipeline.create(options);

    pipeline
      .apply("Read Messages from File", new Input.BoundedData(INPUT))

      .apply("Windowing", Window.into(FixedWindows.of(Duration.standardSeconds(30))))

      .apply("Write to File",
        TextIO
          .write()
          .withWindowedWrites()
          .withNumShards(1)
          .to(options.getOutputPrefix()));

    pipeline.run();
  }
}