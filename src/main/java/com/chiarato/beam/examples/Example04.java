package com.chiarato.beam.examples;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import com.chiarato.beam.examples.utils.ExerciseOptions;
import com.chiarato.beam.examples.utils.Input;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.AvroIO.RecordFormatter;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Contextful;
import org.apache.beam.sdk.transforms.DoFn.RequiresStableInput;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
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
public class Example04 {
  private static final Duration TEN_MINUTES = Duration.standardMinutes(10);

  public static void main(String[] args) throws IOException {
    ExerciseOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(ExerciseOptions.class);

    Pipeline pipeline = Pipeline.create(options);

    String runner = options.getRunner().getSimpleName();
    String topic = options.getTopicName();

    pipeline
      .apply("Read Unbounded Input Data", new Input.UnboundedData(runner, topic))
      .apply("Window Event Types", new WindowedEventTypes())
      .apply("Write File", new WriteEventToAvro(options.getOutputPrefix()));

    pipeline.run().waitUntilFinish();
  }

  /**
   * Windowing unbounded data by Event Type in windows of 5 minutes.
   */
  @SuppressWarnings("serial")
  static class WindowedEventTypes
    extends PTransform<PCollection<String>, PCollection<String>> {

    @Override
    public PCollection<String> expand(PCollection<String> input) {
      return input
        .apply("Windowing",
          Window
            .<String>into(FixedWindows.of(TEN_MINUTES)));
    }
  }

  @SuppressWarnings("serial")
  static class WriteEventToAvro
    extends PTransform<PCollection<String>, PDone>  {

    private String outputPrefix;

    public WriteEventToAvro(String outputPrefix) {
      this.outputPrefix = outputPrefix;
    }

    @Override
    @RequiresStableInput
    public PDone expand(PCollection<String> input) {
      input
        .apply("Write to .avro",
          FileIO
            .<String, String>writeDynamic()
            .by(eventString -> new JSONObject(eventString).getString("event_type"))
            .via(
              Contextful.fn(event -> event),
              AvroIO.sinkViaGenericRecords(
                getSchema(),
                new RecordFormatterFn()))
            .withNaming(eventType -> FileNaming.getNaming(eventType, ".avro"))
            .withDestinationCoder(StringUtf8Coder.of())
            .withCompression(Compression.GZIP)
            .withNumShards(1)
            .to(this.outputPrefix));

      return PDone.in(input.getPipeline());
    }

    private Schema getSchema() {
      String path = "/home/thiago.chiarato/src/beam-examples/src/main/java/com/chiarato/beam/examples/utils/schema.avsc";

      Schema schema = null;
      try {
        schema = new Schema.Parser().parse(new File(path));
      } catch (IOException e) {
        e.printStackTrace();
      }

      return schema;
    }

    static class RecordFormatterFn implements RecordFormatter<String> {

      @Override
      public GenericRecord formatRecord(String element, Schema schema) {
        JSONObject json = new JSONObject(element);
        GenericRecordBuilder record = new GenericRecordBuilder(schema);

        record.set("event_type", json.getString("event_type"));
        record.set("event_timestamp", json.getString("event_timestamp"));
        record.set("payload", json.get("payload").toString());

        return record.build();
		  }
    }

    static class FileNaming implements FileIO.Write.FileNaming {

      private final DateTimeFormatter FILE_FORMATTER = ISODateTimeFormat.hourMinute();
      private final DateTimeFormatter DIR_FORMATTER = DateTimeFormat.forPattern("yyyy/MM/dd/HH");
      private String prefix;
      private String suffix;

      private static FileNaming getNaming(String prefix, String suffix) {
        return new FileNaming(prefix, suffix);
      }

      public FileNaming(String prefix, String suffix) {
        this.prefix = prefix.toLowerCase();
        this.suffix = suffix.toLowerCase();
      }

      @Override
      public String getFilename(
        BoundedWindow window,
        PaneInfo pane,
        int numShards,
        int shardIndex,
        Compression compression) {

        IntervalWindow intervalWindow = (IntervalWindow) window;
        String prefix = filenamePrefixForWindow(intervalWindow);
        String filename =
          String.format(
            "pane-%d-%s-%s-of-%s%s",
            pane.getIndex(),
            pane.getTiming().toString().toLowerCase(),
            shardIndex,
            numShards,
            this.suffix + compression.getSuggestedSuffix());

        return prefix + filename;
      }

      private String filenamePrefixForWindow(IntervalWindow intervalWindow) {
        Instant timestamp = intervalWindow.start();

        return String.format(
          "%s/%s/%s_%s-%s_",
          this.prefix,
          DIR_FORMATTER.print(timestamp),
          UUID.randomUUID(),
          FILE_FORMATTER.print(intervalWindow.start()),
          FILE_FORMATTER.print(intervalWindow.end())
        );
      }
    }
  }
}