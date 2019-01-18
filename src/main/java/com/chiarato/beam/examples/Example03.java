package com.chiarato.beam.examples;

import java.io.IOException;
import java.util.UUID;

import com.chiarato.beam.examples.utils.ExerciseOptions;
import com.chiarato.beam.examples.utils.Input;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;

import org.apache.avro.Schema;
import org.apache.avro.reflect.AvroName;
import org.apache.avro.reflect.ReflectData;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.DefaultFilenamePolicy;
import org.apache.beam.sdk.io.DynamicAvroDestinations;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.FileBasedSink.FilenamePolicy;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Contextful;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
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
public class Example03 {
  private static final Duration TEN_MINUTES = Duration.standardMinutes(10);

  public static void main(String[] args) throws IOException {
    ExerciseOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(ExerciseOptions.class);

    Pipeline pipeline = Pipeline.create(options);

    String runner = options.getRunner().getSimpleName();
    String topic = options.getTopicName();

    pipeline
      .apply("Read Unbounded Input Data", new Input.UnboundedData(runner, topic))
      .apply("Window Event Types", new WindowedEventTypes())
      .apply("Write Data to File", new WriteKVEventToAvro(options.getOutputPrefix()));

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

  @DefaultCoder(AvroCoder.class)
  static class AbstractEvent {
    @AvroName("event_type")
    private String eventType;
    @AvroName("event_timestamp")
    private String eventTimestamp;
    @AvroName("payload")
    private String payload;

    public AbstractEvent() { }

    public AbstractEvent(String eventType, String eventTimestamp, String payload) {
      this.setEventType(eventType);
      this.setEventTimestamp(eventTimestamp);
      this.setPayload(payload);
    }

    /**
     * @return the payload
     */
    public String getPayload() {
      return payload;
    }

    /**
     * @param payload the payload to set
     */
    public void setPayload(String payload) {
      this.payload = payload;
    }

    /**
     * @return the eventTimestamp
     */
    public String getEventTimestamp() {
      return eventTimestamp;
    }

    /**
     * @param eventTimestamp the eventTimestamp to set
     */
    public void setEventTimestamp(String eventTimestamp) {
      this.eventTimestamp = eventTimestamp;
    }

    /**
     * @return the eventType
     */
    public String getEventType() {
      return eventType;
    }

    /**
     * @param eventType the eventType to set
     */
    public void setEventType(String eventType) {
      this.eventType = eventType;
    }
  }

  @SuppressWarnings("serial")
  static class WriteKVEventToAvro
    extends PTransform<PCollection<String>, PDone>  {

    private String outputPrefix;

    public WriteKVEventToAvro(String outputPrefix) {
      this.outputPrefix = outputPrefix;
    }

    @Override
    public PDone expand(PCollection<String> input) {
      ResourceId resource = FileBasedSink.convertToFileResourceIfPossible(this.outputPrefix);

      // input
      //   .apply("Convert to AbstractClass", ParDo.of(new DoFn<String, AbstractEvent>() {
      //     @ProcessElement
      //     public void processElement(ProcessContext context ) throws JsonParseException, JsonMappingException, IOException {
      //       JSONObject json = new JSONObject(context.element());

      //       AbstractEvent event = new AbstractEvent(
      //         json.getString("event_type"),
      //         json.getString("event_timestamp"),
      //         json.get("payload").toString());

      //       context.output(event);
      //     }
      //   }))
      //   .apply("Writer",
      //     TextIO
      //       .<String>writeCustomType()
      //       .withTempDirectory(resource.getCurrentDirectory())
      //       .withWindowedWrites()
      //       .withNumShards(1)
      //       .withSuffix(".txt")
      //       .to(new EventTypeDynamicDestination(this.outputPrefix)));

      input
        .apply("Convert to AbstractClass", ParDo.of(new DoFn<String, AbstractEvent>() {
          @ProcessElement
          public void processElement(ProcessContext context ) throws JsonParseException, JsonMappingException, IOException {
            JSONObject json = new JSONObject(context.element());

            AbstractEvent event = new AbstractEvent(
              json.getString("event_type"),
              json.getString("event_timestamp"),
              json.get("payload").toString());

            context.output(event);
          }
        }))
        .apply("Write to Avro file",
          FileIO
            .<String, AbstractEvent>writeDynamic()
            .by((SerializableFunction<AbstractEvent, String>) event -> event.getEventType())
            .via(
              Contextful.fn(new FormatEvent()),
              AvroIO.sink(AbstractEvent.class))
            .withNaming(eventType -> FileNaming.getNaming(eventType, ".avro"))
            .withDestinationCoder(StringUtf8Coder.of())
            .withNumShards(1)
            .to(this.outputPrefix));
          // AvroIO
          //   .writef(AbstractEvent.class)
          //   .withTempDirectory(resource.getCurrentDirectory())
          //   .withWindowedWrites()
          //   .withNumShards(1)
          //   // .to(this.outputPrefix));
          //   .to(new EventDynamicAvroDestinations(this.outputPrefix)));

      return PDone.in(input.getPipeline());
    }

    static class FormatEvent implements SerializableFunction<AbstractEvent, AbstractEvent> {

      @Override
      public AbstractEvent apply(AbstractEvent input) {
        return input;
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
            this.suffix);

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

    static class EventDynamicAvroDestinations
      extends DynamicAvroDestinations<AbstractEvent, String, AbstractEvent> {

      private String prefix;

      public EventDynamicAvroDestinations(String prefix) {
        this.prefix = prefix;
      }

      @Override
      public Schema getSchema(String destination) {
        return ReflectData.get().getSchema(AbstractEvent.class);
      }

      @Override
      public AbstractEvent formatRecord(AbstractEvent record) {
        return record;
      }

      @Override
      public String getDestination(AbstractEvent element) {
        return element.eventType;
      }

      @Override
      public String getDefaultDestination() {
        // AbstractEvent event = new AbstractEvent();
        // event.eventType = "unkown";
        return "unknow";
      }

      @Override
      public FilenamePolicy getFilenamePolicy(String destination) {
        String path = this.prefix + "/" + destination + "/";
        ResourceId baseFilename = FileBasedSink.convertToFileResourceIfPossible(path);

        return DefaultFilenamePolicy.fromParams(
          new DefaultFilenamePolicy.Params()
            .withBaseFilename(baseFilename)
            .withSuffix(".avro")
            .withWindowedWrites());
      }
    }

    static class EventTypeDynamicDestination
      extends FileBasedSink.DynamicDestinations<AbstractEvent, String, AbstractEvent> {

      private String prefix;

      public EventTypeDynamicDestination(String prefix) {
        this.prefix = prefix;
      }

      @Override
      public AbstractEvent formatRecord(AbstractEvent record) {
        return record;
      }

      @Override
      public String getDestination(AbstractEvent element) {
        // JSONObject json = new JSONObject(element);
        return this.prefix + "/" + element.eventType + "/";
        // return element.eventType;
      }

      @Override
      public String getDefaultDestination() {
        // return this.prefix + "/unknown/";
        return null;
      }

      @Override
      public FilenamePolicy getFilenamePolicy(String destination) {
        // String path = this.prefix + "/" + destination + "/";
        // ResourceId baseFilename = FileBasedSink.convertToFileResourceIfPossible(path);
        ResourceId baseFilename = FileBasedSink.convertToFileResourceIfPossible(destination);

        return DefaultFilenamePolicy.fromParams(
          new DefaultFilenamePolicy.Params()
            .withBaseFilename(baseFilename)
            .withSuffix(".avro")
            .withWindowedWrites());

        // ResourceId resource = FileBasedSink.convertToFileResourceIfPossible(destination);
        // return new WindowedFileNamePolicy(resource);
      }
    }
  }
}