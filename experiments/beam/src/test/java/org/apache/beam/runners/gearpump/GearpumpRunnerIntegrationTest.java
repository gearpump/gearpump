/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.runners.gearpump;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TimestampedValue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.joda.time.Duration;
import org.joda.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

/** Embedded-runner integration tests for the low-level Gearpump Beam runner. */
public class GearpumpRunnerIntegrationTest {

  private static final CopyOnWriteArrayList<String> CAPTURED = new CopyOnWriteArrayList<>();

  private GearpumpPipelineOptions options;

  @BeforeEach
  public void setUp() {
    CAPTURED.clear();
    options = PipelineOptionsFactory.create().as(GearpumpPipelineOptions.class);
    options.setRunner(GearpumpRunner.class);
    options.setApplicationName("beamGearpumpIntegrationTest");
    options.setParallelism(1);
  }

  @AfterEach
  public void tearDown() {
    CAPTURED.clear();
  }

  @Test
  public void runsCreateAndParDoPipelineInEmbeddedCluster() {
    Pipeline pipeline = Pipeline.create(options);
    pipeline
        .apply(Create.of("alpha", "beta"))
        .apply("upper", ParDo.of(new UpperCaseFn()))
        .apply("capture", ParDo.of(new CaptureStringFn()));

    assertPipelineOutputs(pipeline, "ALPHA", "BETA");
  }

  @Test
  public void runsGroupByKeyPipelineInEmbeddedCluster() {
    Pipeline pipeline = Pipeline.create(options);
    pipeline
        .apply(Create.of(KV.of("a", 1), KV.of("a", 2), KV.of("b", 5)))
        .apply(GroupByKey.create())
        .apply("captureSums", ParDo.of(new CaptureGroupedSumsFn()));

    assertPipelineOutputs(pipeline, "a=3", "b=5");
  }

  @Test
  public void runsWindowedGroupByKeyPipelineInEmbeddedCluster() {
    Pipeline pipeline = Pipeline.create(options);
    pipeline
        .apply(
            Create.timestamped(
                TimestampedValue.of(KV.of("a", 1), new Instant(0L)),
                TimestampedValue.of(KV.of("a", 2), new Instant(5_000L)),
                TimestampedValue.of(KV.of("a", 5), new Instant(15_000L))))
        .apply(Window.into(FixedWindows.of(Duration.standardSeconds(10))))
        .apply(GroupByKey.create())
        .apply("captureWindowedSums", ParDo.of(new CaptureGroupedSumsFn()));

    assertPipelineOutputs(pipeline, "a=3", "a=5");
  }

  @Test
  public void rewindowingShouldNotDuplicateElementsAcrossExistingWindows() {
    Pipeline pipeline = Pipeline.create(options);
    pipeline
        .apply(
            Create.timestamped(
                TimestampedValue.of(KV.of("a", 1), new Instant(5_000L))))
        .apply(
            Window.into(
                SlidingWindows.of(Duration.standardSeconds(10))
                    .every(Duration.standardSeconds(5))))
        .apply(Window.into(FixedWindows.of(Duration.standardSeconds(10))))
        .apply(GroupByKey.create())
        .apply("captureRewindowedSums", ParDo.of(new CaptureGroupedSumsFn()));

    assertPipelineOutputs(pipeline, "a=1");
  }

  @Test
  public void runsWindowedGroupByKeyWithEarliestTimestampCombiner() {
    Pipeline pipeline = Pipeline.create(options);
    pipeline
        .apply(
            Create.timestamped(
                TimestampedValue.of(KV.of("a", 1), new Instant(1_000L)),
                TimestampedValue.of(KV.of("a", 2), new Instant(5_000L))))
        .apply(
            Window.<KV<String, Integer>>into(FixedWindows.of(Duration.standardSeconds(10)))
                .withTimestampCombiner(TimestampCombiner.EARLIEST))
        .apply(GroupByKey.create())
        .apply("captureEarliestWindowedSums", ParDo.of(new CaptureTimestampedGroupedSumsFn()));

    assertPipelineOutputs(pipeline, "a@1000=3");
  }

  @Test
  public void runsWindowedGroupByKeyWithLatestTimestampCombiner() {
    Pipeline pipeline = Pipeline.create(options);
    pipeline
        .apply(
            Create.timestamped(
                TimestampedValue.of(KV.of("a", 1), new Instant(1_000L)),
                TimestampedValue.of(KV.of("a", 2), new Instant(5_000L))))
        .apply(
            Window.<KV<String, Integer>>into(FixedWindows.of(Duration.standardSeconds(10)))
                .withTimestampCombiner(TimestampCombiner.LATEST))
        .apply(GroupByKey.create())
        .apply("captureLatestWindowedSums", ParDo.of(new CaptureTimestampedGroupedSumsFn()));

    assertPipelineOutputs(pipeline, "a@5000=3");
  }

  @Test
  public void runsKeyedCombinePipelineInEmbeddedCluster() {
    Pipeline pipeline = Pipeline.create(options);
    pipeline
        .apply(Create.of(KV.of("a", 1), KV.of("a", 2), KV.of("b", 5)))
        .apply(Sum.integersPerKey())
        .apply("captureCombinedSums", ParDo.of(new CaptureCombinedSumsFn()));

    assertPipelineOutputs(pipeline, "a=3", "b=5");
  }

  private static List<String> asSortedList(String... values) {
    List<String> list = new ArrayList<>();
    Collections.addAll(list, values);
    Collections.sort(list);
    return list;
  }

  private static void assertPipelineOutputs(Pipeline pipeline, String... expectedOutputs) {
    GearpumpPipelineResult result = (GearpumpPipelineResult) pipeline.run();
    try {
      waitForOutputs(expectedOutputs.length);
      List<String> actual = new ArrayList<>(CAPTURED);
      Collections.sort(actual);
      assertEquals(asSortedList(expectedOutputs), actual);
    } finally {
      shutdown(result);
    }
  }

  private static void waitForOutputs(int expectedCount) {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (System.nanoTime() < deadline) {
      if (CAPTURED.size() >= expectedCount) {
        return;
      }
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        fail("Interrupted while waiting for Beam pipeline output");
      }
    }
    fail("Timed out waiting for Beam pipeline output. Captured: " + CAPTURED);
  }

  private static void shutdown(GearpumpPipelineResult result) {
    try {
      result.cancel();
    } catch (IOException e) {
      throw new RuntimeException("Failed to cancel Beam test application", e);
    } finally {
      result.getClientContext().close();
    }
  }

  private static final class UpperCaseFn extends DoFn<String, String> {
    @ProcessElement
    public void processElement(ProcessContext context) {
      context.output(context.element().toUpperCase());
    }
  }

  private static final class CaptureStringFn extends DoFn<String, String> {
    @ProcessElement
    public void processElement(ProcessContext context) {
      String value = context.element();
      CAPTURED.add(value);
      context.output(value);
    }
  }

  private static final class CaptureGroupedSumsFn
      extends DoFn<KV<String, Iterable<Integer>>, String> {
    @ProcessElement
    public void processElement(ProcessContext context) {
      KV<String, Iterable<Integer>> element = context.element();
      int sum = 0;
      for (Integer value : element.getValue()) {
        sum += value;
      }
      String output = element.getKey() + "=" + sum;
      CAPTURED.add(output);
      context.output(output);
    }
  }

  private static final class CaptureTimestampedGroupedSumsFn
      extends DoFn<KV<String, Iterable<Integer>>, String> {
    @ProcessElement
    public void processElement(ProcessContext context) {
      KV<String, Iterable<Integer>> element = context.element();
      int sum = 0;
      for (Integer value : element.getValue()) {
        sum += value;
      }
      String output =
          element.getKey() + "@" + context.timestamp().getMillis() + "=" + sum;
      CAPTURED.add(output);
      context.output(output);
    }
  }

  private static final class CaptureCombinedSumsFn extends DoFn<KV<String, Integer>, String> {
    @ProcessElement
    public void processElement(ProcessContext context) {
      KV<String, Integer> element = context.element();
      String output = element.getKey() + "=" + element.getValue();
      CAPTURED.add(output);
      context.output(output);
    }
  }
}
