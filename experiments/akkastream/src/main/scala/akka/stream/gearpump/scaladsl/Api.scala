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

package akka.stream.gearpump.scaladsl

import akka.stream.Attributes
import akka.stream.gearpump.module.{SinkTaskModule, SourceTaskModule, DummySink, DummySource, SinkBridgeModule, SourceBridgeModule}
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import io.gearpump.streaming.dsl.TypedDataSource
import io.gearpump.streaming.sink.DataSink
import io.gearpump.streaming.source.DataSource
import org.reactivestreams.{Publisher, Subscriber}


object GearSource{

  /**
   * Construct a Source which accept out of band input messages.
   *
   *                   [[SourceBridgeModule]] -> Sink
   *                          /
   *                         /
   *                        V
   *                materialize to [[Subscriber]]
   *                                   /|
   *                                  /
   *       upstream [[Publisher]] send out of band message
   *
   */
  def bridge[IN, OUT]: Source[OUT, Subscriber[IN]] = {
    val source = new Source(new DummySource[IN](Attributes.name("dummy"), Source.shape("dummy")))
    val flow = new Flow[IN, OUT, Subscriber[IN]](new SourceBridgeModule[IN, OUT]())
    source.viaMat(flow)(Keep.right)
  }

  /**
   * Construct a Source from Gearpump [[DataSource]].
   *
   *    [[SourceTaskModule]] -> Sink
   *
   */
  def from[OUT](source: DataSource): Source[OUT, Unit] = {
    val taskSource = new Source[OUT, Unit](new SourceTaskModule(source))
    taskSource
  }
}

object GearSink {

  /**
   * Construct a Sink which output messages to out of band channel.
   *
   *   Souce ->   [[SinkBridgeModule]]
   *                    \
   *                     \|
   *         materialize to [[Publisher]]
   *                              \
   *                               \
   *                                \|
   *       send out of band message to downstream [[Subscriber]]
   *
   */
  def bridge[IN, OUT]: Sink[IN, Publisher[OUT]] = {
    val sink = new Sink(new DummySink[OUT](Attributes.name("dummy"), Sink.shape("dummy")))
    val flow = new Flow[IN, OUT, Publisher[OUT]](new SinkBridgeModule[IN, OUT]())
    flow.to(sink)
  }

  /**
   * Construct a Source from Gearpump [[DataSink]].
   *
   *    Source -> [[SinkTaskModule]]
   *
   */
  def to[IN](sink: DataSink): Sink[IN, Unit] = {
    val taskSink = new Sink[IN, Unit](new SinkTaskModule(sink))
    taskSink
  }
}