# Dataflow - Apache Beam Examples

## A Study pourpose project

### Example 01

- In this example, the pipeline was built to read a - **Bounded** - input file, process (5 min Windowed chunks) and write into Text Files.

#### Execution

To run this exercise, open your terminal and execute the following:

```
mvn compile exec:java -Dexec.mainClass="br.com.resultadosdigitais.examples.Example01" -Dexec.args="--runner=DirectRunner --outputPrefix=output/raw/"
```

### Example 02

- In this example, I wrote a pipeline to process **Unbounded** data. This pipeline runs into 2 different ways:
1. *DirectRunner*: Mock data with TestStream.
2. *DataflowRunner*: Subscribe to a Google Cloud Pubub topic.

#### Process

1. The first step of the Pipeline is to extract the event_timestamp from the event payload and output to the `ParDo` Function
2. Then the events are windowed into 5 min chunks.
3. After that Grouped By `event_type` and Reduced into the count of event types.

#### Execution

To run this exercise, open your terminal and execute the following:

```
mvn compile exec:java \
  -Dexec.mainClass="br.com.resultadosdigitais.examples.Example02" \
  -Dexec.args="--runner=DirectRunner \
      --outputPrefix=output/events \
      --topicName=projects/project-name/topics/topic-name"
```
