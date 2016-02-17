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

package io.gearpump.streaming.javaapi.kafka;

import io.gearpump.Message;
import io.gearpump.streaming.javaapi.source.DataSource;
import io.gearpump.streaming.task.TaskContext;
import io.gearpump.streaming.transaction.api.MessageDecoder;
import io.gearpump.streaming.transaction.api.OffsetStorageFactory;
import io.gearpump.streaming.transaction.api.TimeStampFilter;

import java.util.Iterator;
import java.util.Properties;

public class KafkaSource implements DataSource {

  private static final long serialVersionUID = -4185152085413876766L;
  private final io.gearpump.streaming.kafka.KafkaSource underlying;

  public KafkaSource(String topics, Properties properties, OffsetStorageFactory offsetStorageFactory) {
    this(new io.gearpump.streaming.kafka.KafkaSource(topics, properties, offsetStorageFactory));
  }

  public KafkaSource(String topics, Properties properties, OffsetStorageFactory offsetStorageFactory,
      MessageDecoder messageDecoder, TimeStampFilter timeStampFilter) {
    this(new io.gearpump.streaming.kafka.KafkaSource(topics, properties, offsetStorageFactory,
        messageDecoder, timeStampFilter));
  }

  KafkaSource(io.gearpump.streaming.kafka.KafkaSource source) {
    this.underlying = source;
  }

  @Override
  public void open(TaskContext context, final long startTime) {
    underlying.open(context, startTime);
  }

  @Override
  public Iterator<Message> read(final int batchSize) {
    return new Iterator<Message>() {

      private final scala.collection.Iterator<Message> iterator =
          underlying.read(batchSize);

      @Override
      public boolean hasNext() {
        return iterator.hasNext();
      }

      @Override
      public Message next() {
        return iterator.next();
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException(
            "remove API not supported for KafkaSource");
      }
    };
  }

  @Override
  public void close() {
    underlying.close();
  }
}
