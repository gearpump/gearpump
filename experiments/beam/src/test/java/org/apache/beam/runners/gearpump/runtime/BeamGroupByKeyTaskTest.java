/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.runners.gearpump.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.gearpump.DefaultMessage;
import io.gearpump.Message;
import io.gearpump.cluster.ClusterConfig;
import io.gearpump.cluster.UserConfig;
import io.gearpump.streaming.source.Watermark;
import io.gearpump.streaming.task.TaskContext;
import java.lang.reflect.Proxy;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.beam.runners.gearpump.GearpumpRunner;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowedValues;
import org.apache.pekko.actor.ActorSystem;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Watermark and state-lifecycle tests for windowed grouping. */
@SuppressWarnings("unchecked")
public class BeamGroupByKeyTaskTest {
  private ActorSystem system;
  private TaskContext context;
  private final List<WindowedValue<KV<String, Iterable<Integer>>>> outputs = new ArrayList<>();
  private final List<Instant> watermarks = new ArrayList<>();

  @BeforeEach
  public void setUp() {
    system =
        ActorSystem.create(
            "beam-grouping-test",
            GearpumpRunner.configureRunnerConfig(ClusterConfig.defaultConfig(), null));
    context =
        (TaskContext)
            Proxy.newProxyInstance(
                TaskContext.class.getClassLoader(),
                new Class<?>[] {TaskContext.class},
                (proxy, method, args) -> {
                  switch (method.getName()) {
                    case "system":
                      return system;
                    case "appId":
                    case "executorId":
                      return 0;
                    case "taskId":
                    case "self":
                      return null;
                    case "output":
                      outputs.add(
                          (WindowedValue<KV<String, Iterable<Integer>>>)
                              ((Message) args[0]).value());
                      return null;
                    case "updateWatermark":
                      watermarks.add((Instant) args[0]);
                      return null;
                    default:
                      throw new UnsupportedOperationException(method.getName());
                  }
                });
  }

  @AfterEach
  public void tearDown() {
    system.terminate();
  }

  @Test
  public void emitsOnlyClosedWindowsAndNeverReemitsThem() {
    BeamGroupByKeyTask<String, Integer> task = task(TimestampCombiner.END_OF_WINDOW);
    IntervalWindow first = window(0, 10_000);
    IntervalWindow second = window(10_000, 20_000);
    add(task, 1, 1_000, first);
    add(task, 2, 5_000, first);
    add(task, 5, 15_000, second);
    task.onWatermarkProgress(Instant.ofEpochMilli(9_999));
    assertTrue(outputs.isEmpty());
    task.onWatermarkProgress(Instant.ofEpochMilli(10_000));
    assertEquals(1, outputs.size());
    assertOutput(0, first, 9_999, 1, 2);
    add(task, 99, 2_000, first);
    task.onWatermarkProgress(Instant.ofEpochMilli(10_000));
    task.onWatermarkProgress(Instant.ofEpochMilli(5_000));
    assertEquals(1, outputs.size());
    task.onWatermarkProgress(Instant.ofEpochMilli(20_000));
    assertOutput(1, second, 19_999, 5);
    task.onWatermarkProgress(Watermark.MAX());
    task.onWatermarkProgress(Watermark.MAX());
    assertEquals(2, outputs.size());
  }

  @Test
  public void expiresEachSlidingWindowIndependently() {
    BeamGroupByKeyTask<String, Integer> task = task(TimestampCombiner.END_OF_WINDOW);
    IntervalWindow first = window(0, 10_000);
    IntervalWindow second = window(5_000, 15_000);
    add(task, 1, 5_000, first, second);
    task.onWatermarkProgress(Instant.ofEpochMilli(10_000));
    assertOutput(0, first, 9_999, 1);
    // The first assignment is late, but the overlapping second window is still open.
    add(task, 2, 6_000, first, second);
    task.onWatermarkProgress(Instant.ofEpochMilli(15_000));
    assertEquals(2, outputs.size());
    assertOutput(1, second, 14_999, 1, 2);
  }

  @Test
  public void holdsWatermarkForEarliestTimestampUntilEmission() {
    assertTimestampHold(TimestampCombiner.EARLIEST, 1_000);
  }

  @Test
  public void holdsWatermarkForLatestTimestampUntilEmission() {
    assertTimestampHold(TimestampCombiner.LATEST, 5_000);
  }

  @Test
  public void holdsWatermarkForEndOfWindowTimestampUntilEmission() {
    assertTimestampHold(TimestampCombiner.END_OF_WINDOW, 9_999);
  }

  @Test
  public void retainsBoundedGlobalWindowUntilTerminalWatermark() {
    BeamGroupByKeySpec<String> spec =
        new BeamGroupByKeySpec<>(
            StringUtf8Coder.of(), GlobalWindow.Coder.INSTANCE, TimestampCombiner.END_OF_WINDOW);
    BeamGroupByKeyTask<String, Integer> task =
        new BeamGroupByKeyTask<>(
            context,
            BeamUserConfig.withValue(
                UserConfig.empty(), BeamGroupByKeyTask.GROUP_BY_KEY_SPEC, spec, system));
    add(task, 1, 1_000, GlobalWindow.INSTANCE);
    task.onWatermarkProgress(Instant.ofEpochMilli(20_000));
    assertTrue(outputs.isEmpty());
    task.onWatermarkProgress(Watermark.MAX());
    assertEquals(1, outputs.size());
    add(task, 2, 2_000, GlobalWindow.INSTANCE);
    task.onWatermarkProgress(Watermark.MAX());
    assertEquals(1, outputs.size());
  }

  @Test
  public void doesNotRevisitBufferedWindowsOnWatermarkProgress() {
    BeamGroupByKeyTask<String, Integer> task = task(TimestampCombiner.EARLIEST);
    CountingWindow window = new CountingWindow(0, 1_000_000);
    for (int key = 0; key < 1_000; key++) {
      add(task, "key-" + key, key, 1_000, window);
    }
    int initialWindowChecks = window.maxTimestampCalls;
    for (int update = 1; update <= 100; update++) {
      task.onWatermarkProgress(Instant.ofEpochMilli(1_000 + update * 1_000));
    }
    assertTrue(outputs.isEmpty());
    assertWatermark(1_000);
    assertEquals(initialWindowChecks, window.maxTimestampCalls);
    task.onWatermarkProgress(Instant.ofEpochMilli(1_000_000));
    assertEquals(1_000, outputs.size());
    assertEquals(
        1_000L, outputs.stream().map(output -> output.getValue().getKey()).distinct().count());
    assertWatermark(1_000_000);
  }

  @Test
  public void expiresWindowsOutOfArrivalOrderAndRetainsSharedTimestampHolds() {
    BeamGroupByKeyTask<String, Integer> task = task(TimestampCombiner.EARLIEST);
    IntervalWindow first = window(0, 10_000);
    IntervalWindow second = window(0, 20_000);
    add(task, "b", 2, 1_000, second);
    add(task, "a", 1, 1_000, first, second);
    task.onWatermarkProgress(Instant.ofEpochMilli(10_000));
    assertEquals(1, outputs.size());
    assertOutput(0, first, 1_000, 1);
    assertWatermark(1_000);
    task.onWatermarkProgress(Instant.ofEpochMilli(15_000));
    assertEquals(1, outputs.size());
    assertWatermark(1_000);
    task.onWatermarkProgress(Instant.ofEpochMilli(20_000));
    assertEquals(3, outputs.size());
    assertOutput(1, second, 1_000, 2);
    assertOutput(2, second, 1_000, 1);
    assertWatermark(20_000);
  }

  @Test
  public void replacesEarliestHoldWithoutRemovingAnotherGroupsHold() {
    BeamGroupByKeyTask<String, Integer> task = task(TimestampCombiner.EARLIEST);
    IntervalWindow first = window(0, 10_000);
    IntervalWindow second = window(0, 20_000);
    add(task, "a", 1, 5_000, first);
    add(task, "b", 2, 5_000, second);
    add(task, "a", 3, 1_000, first);
    task.onWatermarkProgress(Instant.ofEpochMilli(9_000));
    assertWatermark(1_000);
    task.onWatermarkProgress(Instant.ofEpochMilli(10_000));
    assertOutput(0, first, 1_000, 1, 3);
    assertWatermark(5_000);
    task.onWatermarkProgress(Instant.ofEpochMilli(20_000));
    assertOutput(1, second, 5_000, 2);
    assertWatermark(20_000);
  }

  @Test
  public void advancesLatestHoldWhenPendingGroupTimestampsChange() {
    BeamGroupByKeyTask<String, Integer> task = task(TimestampCombiner.LATEST);
    IntervalWindow first = window(0, 10_000);
    IntervalWindow second = window(0, 20_000);
    add(task, "a", 1, 1_000, first);
    add(task, "a", 2, 5_000, first);
    add(task, "b", 3, 3_000, second);
    task.onWatermarkProgress(Instant.ofEpochMilli(9_000));
    assertWatermark(3_000);
    add(task, "b", 4, 7_000, second);
    task.onWatermarkProgress(Instant.ofEpochMilli(9_500));
    assertWatermark(5_000);
    task.onWatermarkProgress(Instant.ofEpochMilli(10_000));
    assertOutput(0, first, 5_000, 1, 2);
    assertWatermark(7_000);
    task.onWatermarkProgress(Instant.ofEpochMilli(20_000));
    assertOutput(1, second, 7_000, 3, 4);
    assertWatermark(20_000);
  }

  private void assertTimestampHold(TimestampCombiner combiner, long expectedTimestamp) {
    BeamGroupByKeyTask<String, Integer> task = task(combiner);
    IntervalWindow first = window(0, 10_000);
    add(task, 1, 1_000, first);
    add(task, 2, 5_000, first);
    task.onWatermarkProgress(Instant.ofEpochMilli(9_999));
    assertEquals(Instant.ofEpochMilli(expectedTimestamp), watermarks.get(0));
    assertTrue(outputs.isEmpty());
    task.onWatermarkProgress(Instant.ofEpochMilli(10_000));
    assertOutput(0, first, expectedTimestamp, 1, 2);
    assertEquals(Instant.ofEpochMilli(10_000), watermarks.get(1));
  }

  private BeamGroupByKeyTask<String, Integer> task(TimestampCombiner combiner) {
    BeamGroupByKeySpec<String> spec =
        new BeamGroupByKeySpec<>(StringUtf8Coder.of(), IntervalWindow.getCoder(), combiner);
    return new BeamGroupByKeyTask<>(
        context,
        BeamUserConfig.withValue(
            UserConfig.empty(), BeamGroupByKeyTask.GROUP_BY_KEY_SPEC, spec, system));
  }

  private static IntervalWindow window(long start, long end) {
    return new IntervalWindow(new org.joda.time.Instant(start), new org.joda.time.Instant(end));
  }

  private static void add(
      BeamGroupByKeyTask<String, Integer> task,
      int value,
      long timestamp,
      BoundedWindow... windows) {
    add(task, "a", value, timestamp, windows);
  }

  private static void add(
      BeamGroupByKeyTask<String, Integer> task,
      String key,
      int value,
      long timestamp,
      BoundedWindow... windows) {
    WindowedValue<KV<String, Integer>> input =
        WindowedValues.of(
            KV.of(key, value),
            new org.joda.time.Instant(timestamp),
            Arrays.asList(windows),
            PaneInfo.NO_FIRING);
    task.onNext(new DefaultMessage(input, Instant.ofEpochMilli(timestamp)));
  }

  private void assertWatermark(long timestamp) {
    assertEquals(Instant.ofEpochMilli(timestamp), watermarks.get(watermarks.size() - 1));
  }

  private static final class CountingWindow extends IntervalWindow {
    private int maxTimestampCalls;

    private CountingWindow(long start, long end) {
      super(new org.joda.time.Instant(start), new org.joda.time.Instant(end));
    }

    @Override
    public org.joda.time.Instant maxTimestamp() {
      maxTimestampCalls++;
      return super.maxTimestamp();
    }
  }

  private void assertOutput(int index, BoundedWindow window, long timestamp, Integer... values) {
    WindowedValue<KV<String, Iterable<Integer>>> output = outputs.get(index);
    List<Integer> actual = new ArrayList<>();
    output.getValue().getValue().forEach(actual::add);
    assertEquals(Arrays.asList(values), actual);
    assertEquals(Collections.singletonList(window), new ArrayList<>(output.getWindows()));
    assertEquals(timestamp, output.getTimestamp().getMillis());
    assertEquals(PaneInfo.ON_TIME_AND_ONLY_FIRING, output.getPaneInfo());
  }
}
