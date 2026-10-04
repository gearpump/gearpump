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

import io.gearpump.Message;
import io.gearpump.streaming.source.DataSource;
import io.gearpump.streaming.source.Watermark;
import io.gearpump.streaming.task.TaskContext;
import java.time.Instant;

/** Empty Gearpump source used for `Flatten.pCollections()` on an empty input list. */
public class EmptySource implements DataSource {

  @Override
  public void open(TaskContext context, Instant startTime) {}

  @Override
  public Message read() {
    return null;
  }

  @Override
  public void close() {}

  @Override
  public Instant getWatermark() {
    return Watermark.MAX();
  }
}
