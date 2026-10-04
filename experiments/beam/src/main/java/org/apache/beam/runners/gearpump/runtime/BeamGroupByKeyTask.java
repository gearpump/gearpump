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

import io.gearpump.DefaultMessage;
import io.gearpump.Message;
import io.gearpump.cluster.UserConfig;
import io.gearpump.streaming.source.Watermark;
import io.gearpump.streaming.task.Task;
import io.gearpump.streaming.task.TaskContext;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.gearpump.translators.utils.TranslatorUtils;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowedValues;

/**
 * Minimal in-memory Beam GroupByKey task.
 *
 * <p>Emits one final pane per key/window when the input watermark passes the window end,
 * then releases its state. Only default triggers and zero allowed lateness are supported.
 */
@SuppressWarnings("unchecked")
public class BeamGroupByKeyTask<K, V> extends Task {

  public static final String GROUP_BY_KEY_SPEC = "beam.gearpump.group-by-key-spec";

  private final TaskContext taskContext;
  private final BeamGroupByKeySpec<K> spec;
  private final Map<ByteArrayKey, GroupedValues<K, V>> groups = new LinkedHashMap<>();
  private Instant inputWatermark = Watermark.MIN();

  public BeamGroupByKeyTask(TaskContext taskContext, UserConfig userConfig) {
    super(taskContext, userConfig);
    this.taskContext = taskContext;
    this.spec =
        BeamUserConfig.getValue(
            userConfig, GROUP_BY_KEY_SPEC, taskContext.system());
  }

  @Override
  public void onNext(Message message) {
    try {
      WindowedValue<KV<K, V>> windowedValue = (WindowedValue<KV<K, V>>) message.value();
      for (WindowedValue<KV<K, V>> explodedWindow : windowedValue.explodeWindows()) {
        KV<K, V> value = explodedWindow.getValue();
        BoundedWindow window = (BoundedWindow) explodedWindow.getWindows().iterator().next();
        if (!isClosed(window, inputWatermark)) {
          ByteArrayKey key = createGroupingKey(value.getKey(), window);
          GroupedValues<K, V> grouped = groups.get(key);
          if (grouped == null) {
            grouped = new GroupedValues<>(value.getKey(), window);
            groups.put(key, grouped);
          }
          grouped.values.add(value.getValue());
          org.joda.time.Instant timestamp =
              spec.getTimestampCombiner().assign(window, explodedWindow.getTimestamp());
          grouped.timestamp =
              grouped.timestamp == null
                  ? timestamp
                  : spec.getTimestampCombiner().combine(grouped.timestamp, timestamp);
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to group Beam values by key", e);
    }
  }

  @Override
  public void onWatermarkProgress(Instant watermark) {
    if (!watermark.isBefore(inputWatermark)) {
      inputWatermark = watermark;
      Instant outputWatermark = watermark;
      Iterator<GroupedValues<K, V>> iterator = groups.values().iterator();
      while (iterator.hasNext()) {
        GroupedValues<K, V> grouped = iterator.next();
        if (isClosed(grouped.window, watermark)) {
          emit(grouped);
          iterator.remove();
        } else {
          // Pending groups may produce timestamps earlier than the input watermark.
          Instant hold = TranslatorUtils.jodaTimeToJava8Time(grouped.timestamp);
          if (hold.isBefore(outputWatermark)) {
            outputWatermark = hold;
          }
        }
      }
      taskContext.updateWatermark(outputWatermark);
    }
  }

  private static boolean isClosed(BoundedWindow window, Instant watermark) {
    return Watermark.MAX().equals(watermark)
        || watermark.isAfter(TranslatorUtils.jodaTimeToJava8Time(window.maxTimestamp()));
  }

  private void emit(GroupedValues<K, V> grouped) {
    KV<K, Iterable<V>> output = KV.of(grouped.key, (Iterable<V>) new ArrayList<>(grouped.values));
    WindowedValue<KV<K, Iterable<V>>> windowedValue =
        WindowedValues.of(
            output,
            grouped.timestamp,
            Collections.singletonList(grouped.window),
            PaneInfo.ON_TIME_AND_ONLY_FIRING);
    taskContext.output(
        new DefaultMessage(windowedValue, TranslatorUtils.jodaTimeToJava8Time(grouped.timestamp)));
  }

  private ByteArrayKey createGroupingKey(K key, BoundedWindow window) throws IOException {
    byte[] encodedKey = CoderUtils.encodeToByteArray(spec.getKeyCoder(), key);
    Coder<BoundedWindow> windowCoder = (Coder<BoundedWindow>) spec.getWindowCoder();
    byte[] encodedWindow = CoderUtils.encodeToByteArray(windowCoder, window);
    return new ByteArrayKey(encodedKey, encodedWindow);
  }

  private static final class GroupedValues<K, V> {
    private final K key;
    private final BoundedWindow window;
    private final List<V> values = new ArrayList<>();
    private org.joda.time.Instant timestamp;

    private GroupedValues(K key, BoundedWindow window) {
      this.key = key;
      this.window = window;
    }
  }

  private static final class ByteArrayKey {
    private final byte[] keyBytes;
    private final byte[] windowBytes;

    private ByteArrayKey(byte[] keyBytes, byte[] windowBytes) {
      this.keyBytes = keyBytes;
      this.windowBytes = windowBytes;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (!(other instanceof ByteArrayKey)) {
        return false;
      }
      ByteArrayKey that = (ByteArrayKey) other;
      return Arrays.equals(keyBytes, that.keyBytes)
          && Arrays.equals(windowBytes, that.windowBytes);
    }

    @Override
    public int hashCode() {
      return 31 * Arrays.hashCode(keyBytes) + Arrays.hashCode(windowBytes);
    }
  }
}
