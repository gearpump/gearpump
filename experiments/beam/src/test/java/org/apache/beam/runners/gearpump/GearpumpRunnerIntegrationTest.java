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
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
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

  @Test
  public void emitsUnboundedWindowedGroupsWithoutTerminalWatermark() {
    options.setParallelism(2);
    Pipeline pipeline = Pipeline.create(options);
    pipeline.apply(Read.from(new WindowedTestSource()))
        .apply(Window.into(FixedWindows.of(Duration.standardSeconds(10))))
        .apply(GroupByKey.create())
        .apply("captureStreamingSums", ParDo.of(new CaptureGroupedSumsFn()));
    assertPipelineOutputs(pipeline, "a=3", "a=5");
  }

  @Test
  public void emitsChainedUnboundedCombinesWithEarliestTimestamps() {
    Pipeline pipeline = Pipeline.create(options);
    pipeline.apply(Read.from(new WindowedTestSource()))
        .apply(Window.<KV<String, Integer>>into(FixedWindows.of(Duration.standardSeconds(10)))
            .withTimestampCombiner(TimestampCombiner.EARLIEST))
        .apply("firstSum", Sum.integersPerKey())
        .apply("secondSum", Sum.integersPerKey())
        .apply("captureChainedStreamingSums", ParDo.of(new CaptureCombinedSumsFn()));
    assertPipelineOutputs(pipeline, "a=3", "a=5");
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

  /** Emits two windows, then stays idle with a finite watermark instead of ending the source. */
  private static final class WindowedTestSource
      extends UnboundedSource<KV<String, Integer>, TestCheckpoint> {
    @Override
    public List<? extends UnboundedSource<KV<String, Integer>, TestCheckpoint>> split(
        int desiredNumSplits, PipelineOptions options) {
      return Collections.singletonList(this);
    }

    @Override
    public UnboundedReader<KV<String, Integer>> createReader(
        PipelineOptions options, TestCheckpoint checkpoint) {
      return new UnboundedReader<KV<String, Integer>>() {
        private final long[] timestamps = {1_000, 5_000, 15_000};
        private final int[] values = {1, 2, 5};
        private int index;

        @Override
        public boolean start() { return true; }

        @Override
        public boolean advance() {
          if (index < timestamps.length) { index++; }
          return index < timestamps.length;
        }

        @Override
        public KV<String, Integer> getCurrent() { return KV.of("a", values[index]); }

        @Override
        public Instant getCurrentTimestamp() { return new Instant(timestamps[index]); }

        @Override
        public Instant getWatermark() {
          return new Instant(index < timestamps.length ? timestamps[index] : 20_000);
        }

        @Override
        public CheckpointMark getCheckpointMark() { return new TestCheckpoint(); }

        @Override
        public UnboundedSource<KV<String, Integer>, ?> getCurrentSource() {
          return WindowedTestSource.this;
        }

        @Override
        public void close() {}
      };
    }

    @Override
    public Coder<KV<String, Integer>> getOutputCoder() {
      return KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of());
    }

    @Override
    public Coder<TestCheckpoint> getCheckpointMarkCoder() {
      return SerializableCoder.of(TestCheckpoint.class);
    }
  }

  private static final class TestCheckpoint implements UnboundedSource.CheckpointMark, Serializable {
    @Override
    public void finalizeCheckpoint() {}
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
