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
import akka.stream.impl.{SinkModule, SourceModule}
import akka.stream.{Attributes, MaterializationContext, SinkShape, SourceShape}
import org.reactivestreams.{Publisher, Subscriber}

/**
 * [[DummyModule]] is a set of special module to help construct a RunnableGraph,
 * so that all ports are closed.
 *
 * In runtime, [[DummyModule]] should be ignored during materialization.
 *
 * For example, if you have a [[BridgeModule]] which only accept the input
 * message from out of band channel, then you can use DummySource to fake
 * a Message Source Like this.
 *
 * [[DummySource]] -> [[BridgeModule]] -> Sink
 *                      /|
 *                     /
 *       out of band input message [[Publisher]]
 *
 *  After materialization, [[DummySource]] will be removed.

 *              [[BridgeModule]] -> Sink
 *                      /|
 *                     /
 *           [[akka.stream.impl.PublisherSource]]
 *
 *
 */
trait DummyModule extends AtomicModule


/**
 *
 *    [[DummySource]]-> [[BridgeModule]] -> Sink
 *                        /|
 *                       /
 *       out of band input message Source
 *
 * @param attributes Attributes
 * @param shape SourceShape[Out]
 * @tparam Out Output
 */
class DummySource[Out](val attributes: Attributes, shape: SourceShape[Out])
  extends SourceModule[Out, Unit](shape) with DummyModule {

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


/**
 *
 *    Source-> [[BridgeModule]] -> [[DummySink]]
 *                    \
 *                     \
 *                      \|
 *                   out of band output message [[Subscriber]]
 *
 * @param attributes Attributes
 * @param shape SinkShape[IN]
 */
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
