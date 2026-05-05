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
import java.util.Collections;
import java.util.List;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.gearpump.translators.utils.TranslatorUtils;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.Source;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.WindowedValue;

/** Wraps a Beam {@link BoundedSource} as a Gearpump {@link DataSource}. */
public class BoundedSourceWrapper<T> implements DataSource {

  private final SerializablePipelineOptions serializedOptions;
  private final BoundedSource<T> source;

  private transient List<Source.Reader<T>> readers;
  private transient int readerIndex;
  private transient Source.Reader<T> reader;
  private transient boolean available;

  public BoundedSourceWrapper(BoundedSource<T> source, PipelineOptions options) {
    this.source = source;
    this.serializedOptions = new SerializablePipelineOptions(options);
  }

  @Override
  public void open(TaskContext context, Instant startTime) {
    try {
      PipelineOptions options = serializedOptions.get();
      readers = splitReaders(source, options, context);
      readerIndex = 0;
      available = startNextReader();
    } catch (Exception e) {
      close();
      throw new RuntimeException(e);
    }
  }

  @Override
  public Message read() {
    if (!available) {
      return null;
    }
    try {
      T current = reader.getCurrent();
      org.joda.time.Instant timestamp = reader.getCurrentTimestamp();
      Message message =
          new DefaultMessage(
              WindowedValue.timestampedValueInGlobalWindow(current, timestamp),
              TranslatorUtils.jodaTimeToJava8Time(timestamp));
      available = advance();
      return message;
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
      readers = null;
      available = false;
    }
  }

  @Override
  public Instant getWatermark() {
    return available ? Watermark.MIN() : Watermark.MAX();
  }

  private List<Source.Reader<T>> splitReaders(
      BoundedSource<T> source, PipelineOptions options, TaskContext context) throws Exception {
    int parallelism = Math.max(1, context.parallelism());
    long estimatedSize = Math.max(1L, source.getEstimatedSizeBytes(options));
    long desiredBundleSize = Math.max(1L, estimatedSize / parallelism);
    List<? extends BoundedSource<T>> splits = source.split(desiredBundleSize, options);
    if (splits == null || splits.isEmpty()) {
      splits = Collections.singletonList(source);
    }

    List<Source.Reader<T>> assigned = new ArrayList<>();
    for (int i = 0; i < splits.size(); i++) {
      if (i % parallelism == context.taskId().index()) {
        assigned.add(splits.get(i).createReader(options));
      }
    }
    return assigned;
  }

  private boolean startNextReader() throws Exception {
    while (readerIndex < readers.size()) {
      reader = readers.get(readerIndex++);
      if (reader.start()) {
        return true;
      }
      reader.close();
      reader = null;
    }
    return false;
  }

  private boolean advance() throws Exception {
    if (reader != null && reader.advance()) {
      return true;
    }
    if (reader != null) {
      reader.close();
      reader = null;
    }
    return startNextReader();
  }
}
