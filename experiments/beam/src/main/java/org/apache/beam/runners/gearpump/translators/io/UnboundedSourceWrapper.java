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
import java.util.Collections;
import java.util.List;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.gearpump.translators.utils.TranslatorUtils;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.values.WindowedValues;

/** Wraps a Beam {@link UnboundedSource} as a Gearpump {@link DataSource}. */
public class UnboundedSourceWrapper<T> implements DataSource {

  private final SerializablePipelineOptions serializedOptions;
  private final UnboundedSource<T, ?> source;

  private transient UnboundedSource.UnboundedReader<T> reader;
  private transient boolean available;

  public UnboundedSourceWrapper(UnboundedSource<T, ?> source, PipelineOptions options) {
    this.source = source;
    this.serializedOptions = new SerializablePipelineOptions(options);
  }

  @Override
  public void open(TaskContext context, Instant startTime) {
    try {
      PipelineOptions options = serializedOptions.get();
      UnboundedSource<T, ?> assigned = assignSource(source, options, context);
      if (assigned != null) {
        reader = assigned.createReader(options, null);
        available = reader.start();
      }
    } catch (Exception e) {
      close();
      throw new RuntimeException(e);
    }
  }

  @Override
  public Message read() {
    if (reader == null) {
      return null;
    }
    try {
      if (!available) {
        available = reader.advance();
        if (!available) {
          return null;
        }
      }

      T current = reader.getCurrent();
      org.joda.time.Instant timestamp = reader.getCurrentTimestamp();
      available = false;
      return new DefaultMessage(
          WindowedValues.timestampedValueInGlobalWindow(current, timestamp),
          TranslatorUtils.jodaTimeToJava8Time(timestamp));
    } catch (Exception e) {
      close();
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() {
    try {
      if (reader != null) {
        reader.close();
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      reader = null;
      available = false;
    }
  }

  @Override
  public Instant getWatermark() {
    if (reader == null) {
      return Watermark.MAX();
    }
    return TranslatorUtils.jodaTimeToJava8Time(reader.getWatermark());
  }

  private UnboundedSource<T, ?> assignSource(
      UnboundedSource<T, ?> source, PipelineOptions options, TaskContext context) throws Exception {
    int parallelism = Math.max(1, context.parallelism());
    List<? extends UnboundedSource<T, ?>> splits = source.split(parallelism, options);
    if (splits == null || splits.isEmpty()) {
      splits = Collections.singletonList(source);
    }

    int taskIndex = context.taskId().index();
    for (int i = 0; i < splits.size(); i++) {
      if (i % parallelism == taskIndex) {
        return splits.get(i);
      }
    }
    return null;
  }
}
