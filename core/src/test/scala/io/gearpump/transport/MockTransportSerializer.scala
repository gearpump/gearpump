/*
 * Licensed under the Apache License, Version 2.0 (the
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

package io.gearpump.transport

import io.gearpump.transport.MockTransportSerializer.NettyMessage
import io.gearpump.transport.netty.ITransportMessageSerializer
import java.io.{DataInput, DataOutput}

class MockTransportSerializer extends ITransportMessageSerializer {
  override def getLength(obj: scala.Any): Int = 4

  override def serialize(dataOutput: DataOutput, transportMessage: scala.Any): Unit = {
    transportMessage match {
      case msg: NettyMessage =>
        dataOutput.writeInt(msg.num)
    }
  }

  override def deserialize(dataInput: DataInput, length: Int): AnyRef = {
    NettyMessage(dataInput.readInt())
  }
}

object MockTransportSerializer {
  case class NettyMessage(num: Int)
}
