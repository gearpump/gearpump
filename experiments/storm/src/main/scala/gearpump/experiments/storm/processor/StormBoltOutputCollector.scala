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

package gearpump.experiments.storm.processor

import java.util.{Collection => JCollection, List => JList}

import backtype.storm.task.IOutputCollector
import backtype.storm.tuple.Tuple

private[storm] class StormBoltOutputCollector(outputFn: (String, JList[AnyRef]) => Unit) extends IOutputCollector {

  override def emit(streamId: String, anchors: JCollection[Tuple], tuple: JList[AnyRef]): JList[Integer] = {
    outputFn(streamId, tuple)
    null
  }

  override def emitDirect(i: Int, s: String, collection: JCollection[Tuple], list: JList[AnyRef]): Unit = {
    throw new RuntimeException("emit direct not supported")
  }

  override def fail(tuple: Tuple): Unit = {}

  override def ack(tuple: Tuple): Unit = {}

  override def reportError(throwable: Throwable): Unit = {
    throw throwable
  }
}
