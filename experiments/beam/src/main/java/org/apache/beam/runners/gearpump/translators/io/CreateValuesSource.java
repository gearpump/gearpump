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
package org.apache.beam.runners.gearpump.translators.io;

import io.gearpump.DefaultMessage;
import io.gearpump.Message;
import io.gearpump.streaming.source.DataSource;
import io.gearpump.streaming.source.Watermark;
import io.gearpump.streaming.task.TaskContext;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowedValues;

/** Wraps Beam {@code Create.of(...)} values as a Gearpump {@link DataSource}. */
public class CreateValuesSource<T> implements DataSource {

  private final Coder<T> coder;
  private final List<byte[]> encodedElements;

  private transient int currentIndex;
  private transient int step;

  public CreateValuesSource(List<T> elements, Coder<T> coder) {
    this.coder = coder;
    this.encodedElements = new ArrayList<>(elements.size());
    try {
      for (T element : elements) {
        encodedElements.add(CoderUtils.encodeToByteArray(coder, element));
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to encode Beam Create values", e);
    }
  }

  @Override
  public void open(TaskContext context, Instant startTime) {
    currentIndex = context.taskId().index();
    step = Math.max(1, context.parallelism());
  }

  @Override
  public Message read() {
    if (currentIndex >= encodedElements.size()) {
      return null;
    }
    try {
      T value = CoderUtils.decodeFromByteArray(coder, encodedElements.get(currentIndex));
      currentIndex += step;
      WindowedValue<T> windowedValue = WindowedValues.valueInGlobalWindow(value);
      return new DefaultMessage(
          windowedValue, Instant.ofEpochMilli(windowedValue.getTimestamp().getMillis()));
    } catch (Exception e) {
      throw new RuntimeException("Failed to decode Beam Create value", e);
    }
  }

  @Override
  public void close() {}

  @Override
  public Instant getWatermark() {
    return currentIndex >= encodedElements.size() ? Watermark.MAX() : Watermark.MIN();
  }
}
