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

import java.io.{DataInput, DataOutput}

class SerializedMessageSerializer extends TaskMessageSerializer[SerializedMessage] {
  override def getLength(obj: SerializedMessage): Int = 12 + obj.bytes.length

  override def write(dataOutput: DataOutput, obj: SerializedMessage): Unit = {
    dataOutput.writeLong(obj.timeStamp)
    dataOutput.writeInt(obj.bytes.length)
    dataOutput.write(obj.bytes)
  }

  override def read(dataInput: DataInput): SerializedMessage = {
    val timestamp = dataInput.readLong()
    val length = dataInput.readInt()
    val bytes = new Array[Byte](length)
    dataInput.readFully(bytes)
    SerializedMessage(timestamp, bytes)
  }
}
