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

package akka.stream.gearpump.task

import akka.stream.gearpump.task.UnZip2Task.UnZipFunction
import io.gearpump.Message
import io.gearpump.cluster.UserConfig
import io.gearpump.streaming.task.TaskContext

class UnZip2Task(context: TaskContext, userConf : UserConfig) extends GraphTask(context, userConf) {

  val unzip = userConf.getValue[UnZipFunction](UnZip2Task.UNZIP2_FUNCTION)(context.system).get.unzip

  override def onNext(msg : Message) : Unit = {
    val message = msg.msg
    val time = msg.timestamp
    val pair = unzip(message)
    val (a, b) = pair
    output(0, Message(a.asInstanceOf[AnyRef], time))
    output(1, Message(b.asInstanceOf[AnyRef], time))
  }
}

object UnZip2Task {
  class UnZipFunction(val unzip: Any => (Any, Any)) extends Serializable

  val UNZIP2_FUNCTION = "akka.stream.gearpump.task.unzip2.function"
}