package com.chiarato.beam.examples.utils;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

/**
 * ExerciseOptions
 */
public interface ExerciseOptions extends PipelineOptions {

    @Description("Topic which the code will listen for incoming messages")
    @Default.String("projects/data-lake-demo/topics/application-data")
    String getTopicName();

    void setTopicName(String value);

    @Description("Prefix for output files, either local path or cloud storage location")
    @Default.String("output/")
    String getOutputPrefix();

    void setOutputPrefix(String value);
}