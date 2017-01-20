/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gearpump.akkastream.module

import akka.stream.impl.StreamLayout.{AtomicModule, Module}
import akka.stream._
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.streaming.sink.DataSink
import org.apache.gearpump.streaming.source.DataSource
import org.apache.gearpump.streaming.task.Task

/**
 * [[GearpumpTaskModule]] represent modules that can be materialized as Gearpump Tasks.
 *
 * This is specially designed for Gearpump runtime. It is not supposed to be used
 * for local materializer.
 * 
 */
trait GearpumpTaskModule extends AtomicModule

/**
 * This is used to represent the Gearpump Data Source
 * @param source DataSource
 * @param conf UserConfig
 * @param shape SourceShape[T}
 * @param attributes Attributes
 * @tparam T type
 */
final case class SourceTaskModule[T](
    source: DataSource,
    conf: UserConfig,
    shape: SourceShape[T] = SourceShape[T](Outlet[T]("SourceTaskModule.out")),
    attributes: Attributes = Attributes.name("SourceTaskModule"))
  extends GearpumpTaskModule {

  override def withAttributes(attr: Attributes): Module =
    this.copy(shape = amendShape(attr), attributes = attr)
  override def carbonCopy: Module =
    this.copy(shape = SourceShape( Outlet[T]("SourceTaskModule.out")))

  override def replaceShape(s: Shape): Module =
    if (s == shape) this
    else throw new UnsupportedOperationException("cannot replace the shape of SourceTaskModule")

  private def amendShape(attr: Attributes): SourceShape[T] = {
    val thisN = attributes.nameOrDefault(null)
    val thatN = attr.nameOrDefault(null)

    if ((thatN eq null) || thisN == thatN) shape
    else shape.copy(out = Outlet(thatN + ".out"))
  }
}

/**
 * This is used to represent the Gearpump Data Sink
 * @param sink DataSink
 * @param conf UserConfig
 * @param shape SinkShape[IN]
 * @param attributes Attributes
 * @tparam IN type
 */
final case class SinkTaskModule[IN](
    sink: DataSink,
    conf: UserConfig,
    shape: SinkShape[IN] = SinkShape[IN](Inlet[IN]("SinkTaskModule.in")),
    attributes: Attributes = Attributes.name("SinkTaskModule"))
  extends GearpumpTaskModule {

  override def withAttributes(attr: Attributes): Module =
    this.copy(shape = amendShape(attr), attributes = attr)
  override def carbonCopy: Module =
    this.copy(shape = SinkShape(Inlet[IN]("SinkTaskModule.in")))

  override def replaceShape(s: Shape): Module =
    if (s == shape) this
    else throw new UnsupportedOperationException("cannot replace the shape of SinkTaskModule")

  private def amendShape(attr: Attributes): SinkShape[IN] = {
    val thisN = attributes.nameOrDefault(null)
    val thatN = attr.nameOrDefault(null)

    if ((thatN eq null) || thisN == thatN) shape
    else shape.copy(in = Inlet(thatN + ".out"))
  }
}

/**
 * This is to represent the Gearpump Processor which has exact one input and one output
 * @param processor Class[_ <: Task]
 * @param conf UserConfig
 * @param attributes Attributes
 * @tparam IN type
 * @tparam OUT type
 * @tparam Unit void
 */
case class ProcessorModule[IN, OUT, Unit](
    processor: Class[_ <: Task],
    conf: UserConfig,
    attributes: Attributes = Attributes.name("processorModule"))
  extends AtomicModule with GearpumpTaskModule {
  val inPort = Inlet[IN]("ProcessorModule.in")
  val outPort = Outlet[IN]("ProcessorModule.out")
  override val shape = new FlowShape(inPort, outPort)

  override def replaceShape(s: Shape): Module = if (s != shape) {
    throw new UnsupportedOperationException("cannot replace the shape of a FlowModule")
  } else {
    this
  }
  
  override def carbonCopy: Module = newInstance

  protected def newInstance: ProcessorModule[IN, OUT, Unit] =
    new ProcessorModule[IN, OUT, Unit](processor, conf, attributes)

  override def withAttributes(attributes: Attributes): ProcessorModule[IN, OUT, Unit] = {
    new ProcessorModule[IN, OUT, Unit](processor, conf, attributes)
  }
}
