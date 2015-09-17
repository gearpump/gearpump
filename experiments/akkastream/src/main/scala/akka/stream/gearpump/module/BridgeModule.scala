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

import akka.stream.Attributes
import akka.stream.impl.FlowModule
import akka.stream.impl.StreamLayout.Module
import org.reactivestreams.Subscriber

abstract class BridgeModule[IN, OUT, MAT] extends FlowModule[IN, OUT, MAT]{
  def attributes: Attributes
  def withAttributes(attributes: Attributes): BridgeModule[IN, OUT, MAT]

  protected def newInstance: BridgeModule[IN, OUT, MAT]
  override def carbonCopy: Module = newInstance
}

object BridgeModule {
  class SourceBridgeModule[IN, OUT](val attributes: Attributes = Attributes.name("sourceBridgeModule")) extends BridgeModule[IN, OUT, Subscriber[IN]] {
    override protected def newInstance: BridgeModule[IN, OUT, Subscriber[IN]] = new SourceBridgeModule[IN, OUT](attributes)

    override def withAttributes(attributes: Attributes): BridgeModule[IN, OUT, Subscriber[IN]] = {
      new SourceBridgeModule( attributes)
    }
  }

  class SinkBridgeModule[IN, OUT](val attributes: Attributes = Attributes.name("sourceBridgeModule")) extends BridgeModule[IN, OUT, Subscriber[IN]] {
    override protected def newInstance: BridgeModule[IN, OUT, Subscriber[IN]] = new SinkBridgeModule(attributes)

    override def withAttributes(attributes: Attributes): BridgeModule[IN, OUT, Subscriber[IN]] = {
      new SinkBridgeModule(attributes)
    }
  }
}
