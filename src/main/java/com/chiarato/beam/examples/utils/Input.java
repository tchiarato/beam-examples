package com.chiarato.beam.examples.utils;

import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

/**
 * Input
 */
public class Input {

  public static class BoundedData extends PTransform<PBegin, PCollection<String>> {

    private String fileName;

    public BoundedData(String fileName) {
      this.fileName = fileName;
    }

    @Override
    public PCollection<String> expand(PBegin input) {
      return input.apply(
        TextIO
          .read()
          .from(fileName));
    }
  }
}