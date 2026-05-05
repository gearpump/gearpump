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
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.gearpump.translators.utils.TranslatorUtils;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;

/**
 * Minimal in-memory Beam GroupByKey task.
 *
 * <p>This implementation intentionally supports the default global window only and emits grouped
 * values when the input watermark reaches {@link Watermark#MAX()}.
 */
@SuppressWarnings("unchecked")
public class BeamGroupByKeyTask<K, V> extends Task {

  public static final String GROUP_BY_KEY_SPEC = "beam.gearpump.group-by-key-spec";

  private final TaskContext taskContext;
  private final BeamGroupByKeySpec<K> spec;
  private final Map<ByteArrayKey, GroupedValues<K, V>> groups = new LinkedHashMap<>();
  private boolean emitted = false;

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
      KV<K, V> value = windowedValue.getValue();
      byte[] encodedKey = CoderUtils.encodeToByteArray(spec.getKeyCoder(), value.getKey());
      ByteArrayKey key = new ByteArrayKey(encodedKey);
      GroupedValues<K, V> grouped = groups.get(key);
      if (grouped == null) {
        grouped = new GroupedValues<>(value.getKey());
        groups.put(key, grouped);
      }
      grouped.values.add(value.getValue());
    } catch (Exception e) {
      throw new RuntimeException("Failed to group Beam values by key", e);
    }
  }

  @Override
  public void onWatermarkProgress(Instant watermark) {
    if (!emitted && Watermark.MAX().equals(watermark)) {
      emitAll();
      emitted = true;
      groups.clear();
    }
    taskContext.updateWatermark(watermark);
  }

  private void emitAll() {
    org.joda.time.Instant outputTimestamp = GlobalWindow.INSTANCE.maxTimestamp();
    Instant javaTimestamp = TranslatorUtils.jodaTimeToJava8Time(outputTimestamp);
    for (GroupedValues<K, V> grouped : groups.values()) {
      KV<K, Iterable<V>> output = KV.of(grouped.key, (Iterable<V>) new ArrayList<>(grouped.values));
      WindowedValue<KV<K, Iterable<V>>> windowedValue =
          WindowedValue.timestampedValueInGlobalWindow(output, outputTimestamp);
      taskContext.output(new DefaultMessage(windowedValue, javaTimestamp));
    }
  }

  private static final class GroupedValues<K, V> {
    private final K key;
    private final List<V> values = new ArrayList<>();

    private GroupedValues(K key) {
      this.key = key;
    }
  }

  private static final class ByteArrayKey {
    private final byte[] value;

    private ByteArrayKey(byte[] value) {
      this.value = value;
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
      return Arrays.equals(value, that.value);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(value);
    }
  }
}
