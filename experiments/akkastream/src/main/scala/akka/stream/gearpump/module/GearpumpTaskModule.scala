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

package akka.stream.gearpump.module

import akka.stream.{Inlet, SinkShape, Shape, Outlet, SourceShape, Attributes}
import akka.stream.impl.{StreamLayout, FlowModule}
import akka.stream.impl.StreamLayout.Module
import akka.stream.impl.StreamLayout.Module
import io.gearpump.streaming.sink.DataSink
import io.gearpump.streaming.source.DataSource
import org.reactivestreams.{Publisher, Subscriber}

/**
 * [[GearpumpTaskModule]] represent modules that can be materialized as Gearpump Tasks.
 * 
 */

trait GearpumpTaskModule extends Module

final case class SourceTaskModule[T](
   source: DataSource,
   shape: SourceShape[T] = SourceShape[T](Outlet[T]("SourceTaskModule.out")),
   attributes: Attributes = Attributes.name("SourceTaskModule"))
  extends GearpumpTaskModule {

  override def subModules: Set[Module] = Set.empty
  override def withAttributes(attr: Attributes): Module = this.copy(shape = amendShape(attr), attributes = attr)
  override def carbonCopy: Module = this.copy(shape = SourceShape(Outlet[T]("SourceTaskModule.out")))

  override def replaceShape(s: Shape): Module =
    if (s == shape) this
    else throw new UnsupportedOperationException("cannot replace the shape of SourceTaskModule")

  private def amendShape(attr: Attributes): SourceShape[T] = {
    val thisN = attributes.nameOrDefault(null)
    val thatN = attr.nameOrDefault(null)

    if ((thatN eq null) || thisN == thatN) shape
    else shape.copy(outlet = Outlet(thatN + ".out"))
  }
}

final case class SinkTaskModule[IN](
    sink: DataSink,
    shape: SinkShape[IN] = SinkShape[IN](Inlet[IN]("SinkTaskModule.in")),
    attributes: Attributes = Attributes.name("SinkTaskModule"))
  extends GearpumpTaskModule {

  override def subModules: Set[Module] = Set.empty
  override def withAttributes(attr: Attributes): Module = this.copy(shape = amendShape(attr), attributes = attr)
  override def carbonCopy: Module = this.copy(shape = SinkShape(Inlet[IN]("SinkTaskModule.in")))

  override def replaceShape(s: Shape): Module =
    if (s == shape) this
    else throw new UnsupportedOperationException("cannot replace the shape of SinkTaskModule")

  private def amendShape(attr: Attributes): SinkShape[IN] = {
    val thisN = attributes.nameOrDefault(null)
    val thatN = attr.nameOrDefault(null)

    if ((thatN eq null) || thisN == thatN) shape
    else shape.copy(inlet = Inlet(thatN + ".out"))
  }
}