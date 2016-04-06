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

package akka.stream.gearpump.module

import akka.stream.Attributes
import akka.stream.impl.FlowModule
import akka.stream.impl.StreamLayout.Module
import org.reactivestreams.{Publisher, Subscriber}

/**
 *
 *
 * [[IN]] -> [[BridgeModule]] -> [[OUT]]
 * /
 * /
 * out of band data input or output channel [[MAT]]
 *
 *
 * [[BridgeModule]] is used as a bridge between different materializers.
 * Different [[akka.stream.Materializer]]s can use out of band channel to
 * exchange messages.
 *
 * For example:
 *
 * Remote Materializer
 * -----------------------------
 * |                            |
 * | BridgeModule -> RemoteSink |
 * |  /                         |
 * --/----------------------------
 * Local Materializer     /  out of band channel.
 * ----------------------/----
 * | Local              /    |
 * | Source ->  BridgeModule |
 * |                         |
 * ---------------------------
 *
 *
 * Typically [[BridgeModule]] is created implicitly as a temporary intermediate
 * module during materialization.
 *
 * However, user can still declare it explicitly. In this case, it means we have a
 * boundary Source or Sink module which accept out of band channel inputs or
 * outputs.
 *
 *
 * @tparam IN
 * @tparam OUT
 * @tparam MAT
 */
abstract class BridgeModule[IN, OUT, MAT] extends FlowModule[IN, OUT, MAT] {
  def attributes: Attributes
  def withAttributes(attributes: Attributes): BridgeModule[IN, OUT, MAT]

  protected def newInstance: BridgeModule[IN, OUT, MAT]
  override def carbonCopy: Module = newInstance
}


/**
 *
 * Bridge module which accept out of band channel Input
 * [[org.reactivestreams.Publisher]][IN].
 *
 *
 * [[SourceBridgeModule]] -> [[OUT]]
 * /|
 * /
 * out of band data input [[org.reactivestreams.Publisher]][IN]
 *
 * @see [[BridgeModule]]
 *
 * @param attributes
 * @tparam IN, input data type from out of band [[org.reactivestreams.Publisher]]
 * @tparam OUT out put data type to next module.
 */
class SourceBridgeModule[IN, OUT](val attributes: Attributes = Attributes.name("sourceBridgeModule")) extends BridgeModule[IN, OUT, Subscriber[IN]] {
  override protected def newInstance: BridgeModule[IN, OUT, Subscriber[IN]] = new SourceBridgeModule[IN, OUT](attributes)

  override def withAttributes(attributes: Attributes): BridgeModule[IN, OUT, Subscriber[IN]] = {
    new SourceBridgeModule(attributes)
  }
}

/**
 *
 * Bridge module which accept out of band channel Output
 * [[org.reactivestreams.Subscriber]][OUT].
 *
 *
 * [[IN]] -> [[BridgeModule]]
 * \
 * \
 * \|
 * out of band data output [[org.reactivestreams.Subscriber]][OUT]
 *
 * @see [[BridgeModule]]
 *
 * @param attributes
 * @tparam IN, input data type from previous module
 * @tparam OUT out put data type to out of band subscriber
 */
class SinkBridgeModule[IN, OUT](val attributes: Attributes = Attributes.name("sourceBridgeModule")) extends BridgeModule[IN, OUT, Publisher[OUT]] {
  override protected def newInstance: BridgeModule[IN, OUT, Publisher[OUT]] = new SinkBridgeModule[IN, OUT](attributes)

  override def withAttributes(attributes: Attributes): BridgeModule[IN, OUT, Publisher[OUT]] = {
    new SinkBridgeModule[IN, OUT](attributes)
  }
}