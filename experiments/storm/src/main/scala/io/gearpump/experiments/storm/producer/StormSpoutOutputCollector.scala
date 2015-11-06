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

package io.gearpump.experiments.storm.producer

import java.util.{List => JList}

import backtype.storm.spout.ISpoutOutputCollector
import io.gearpump.experiments.storm.util.StormOutputCollector

/**
 * this is used by Storm Spout to emit messages
 */
private[storm] class StormSpoutOutputCollector(collector: StormOutputCollector) extends ISpoutOutputCollector {

  override def emit(streamId: String, values: JList[AnyRef], messageId: scala.Any): JList[Integer] = {
    collector.emit(streamId, values)
  }

  override def reportError(throwable: Throwable): Unit = {
    throw throwable
  }

  override def emitDirect(taskId: Int, streamId: String, values: JList[AnyRef], messageId: scala.Any): Unit = {
    collector.emitDirect(taskId, streamId, values)
  }
}
