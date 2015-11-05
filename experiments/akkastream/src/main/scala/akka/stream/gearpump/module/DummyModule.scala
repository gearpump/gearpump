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

import akka.stream.impl.StreamLayout.Module
import akka.stream.impl.{SinkModule, SourceModule}
import akka.stream.{Attributes, MaterializationContext, SinkShape, SourceShape}
import org.reactivestreams.{Publisher, Subscriber}

trait DummyModule extends Module

object DummyMoule {
  class DummySource[Out](val attributes: Attributes, shape: SourceShape[Out])
    extends SourceModule[Out, Unit](shape) with DummyModule {

    // TODO: the materialization process is finished in Materializer, instead of here
    override def create(context: MaterializationContext): (Publisher[Out], Unit) = {
      throw new UnsupportedOperationException()
    }

    override protected def newInstance(shape: SourceShape[Out]): SourceModule[Out, Unit] = {
      new DummySource[Out](attributes, shape)
    }

    override def withAttributes(attr: Attributes): Module = {
      new DummySource(attr, amendShape(attr))
    }
  }

  class DummySink[IN](val attributes: Attributes, shape: SinkShape[IN])
    extends SinkModule[IN, Unit](shape) with DummyModule {
    override def create(context: MaterializationContext): (Subscriber[IN], Unit) = {
      throw new UnsupportedOperationException()
    }

    override protected def newInstance(shape: SinkShape[IN]): SinkModule[IN, Unit] = {
      new DummySink[IN](attributes, shape)
    }

    override def withAttributes(attr: Attributes): Module = {
      new DummySink[IN](attr, amendShape(attr))
    }
  }
}