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

package io.gearpump.streaming.javaapi.source;

import io.gearpump.Message;
import io.gearpump.streaming.task.TaskContext;

import java.io.Serializable;
import java.util.Iterator;

public interface DataSource extends Serializable {

  /**
   * open connection to data source
   * invoked in onStart() method of [[io.gearpump.streaming.javaapi.source.DataSourceTask]]
   * @param context is the task context at runtime
   * @param startTime is the start time of system
   */
  void open(TaskContext context, final long startTime);

  /**
   * read a number of messages from data source.
   * invoked in each onNext() method of [[io.gearpump.streaming.javaapi.source.DataSourceTask]]
   * @param batchSize max number of messages to read
   * @return a list of messages wrapped in [[io.gearpump.Message]]
   */
  Iterator<Message> read(final int batchSize);

  /**
   * close connection to data source.
   * invoked in onStop() method of [[io.gearpump.streaming.javaapi.source.DataSourceTask]]
   */
  void close();
}
