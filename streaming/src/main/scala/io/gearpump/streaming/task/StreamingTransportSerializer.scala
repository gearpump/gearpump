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
package io.gearpump.streaming.task

import io.gearpump.streaming.{AckRequestSerializer, AckSerializer, InitialAckRequestSerializer, LatencyProbeSerializer}
import io.gearpump.transport.netty.ITransportMessageSerializer
import io.gearpump.util.LogUtil
import java.io.{DataInput, DataOutput}
import org.slf4j.Logger

class StreamingTransportSerializer extends ITransportMessageSerializer {
  private val log: Logger = LogUtil.getLogger(getClass)
  private val serializers = new SerializerResolver

  serializers.register(classOf[Ack], new AckSerializer)
  serializers.register(classOf[AckRequest], new AckRequestSerializer)
  serializers.register(classOf[InitialAckRequest], new InitialAckRequestSerializer)
  serializers.register(classOf[LatencyProbe], new LatencyProbeSerializer)
  serializers.register(classOf[SerializedMessage], new SerializedMessageSerializer)

  override def serialize(dataOutput: DataOutput, obj: Object): Unit = {
    val registration = serializers.getRegistration(obj.getClass)
    if (registration != null) {
      dataOutput.writeInt(registration.id)
      registration.serializer.asInstanceOf[TaskMessageSerializer[AnyRef]].write(dataOutput, obj)
    } else {
      log.error(s"Can not find serializer for class type ${obj.getClass}")
    }
  }

  override def deserialize(dataInput: DataInput, length: Int): Object = {
    val classID = dataInput.readInt()
    val registration = serializers.getRegistration(classID)
    if (registration != null) {
      registration.serializer.asInstanceOf[TaskMessageSerializer[AnyRef]].read(dataInput)
    } else {
      log.error(s"Can not find serializer for class id $classID")
      null
    }
  }

  override def getLength(obj: Object): Int = {
    val registration = serializers.getRegistration(obj.getClass)
    if (registration != null) {
      registration.serializer.asInstanceOf[TaskMessageSerializer[AnyRef]].getLength(obj) + 4
    } else {
      log.error(s"Can not find serializer for class type ${obj.getClass}")
      0
    }
  }
}
