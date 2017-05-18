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

package org.apache.gearpump.akkastream.task

import org.apache.gearpump.akkastream.task.Unzip2Task.UnZipFunction
import org.apache.gearpump.Message
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.streaming.task.TaskContext

class Unzip2Task[In, A1, A2](context: TaskContext, userConf : UserConfig)
  extends GraphTask(context, userConf) {

  val unzip = userConf.
    getValue[UnZipFunction[In, A1, A2]](Unzip2Task.UNZIP2_FUNCTION)(context.system).get.unzip

  override def onNext(msg: Message) : Unit = {
    val value = msg.value
    val time = msg.timestamp
    val pair = unzip(value.asInstanceOf[In])
    val (a, b) = pair
    output(0, Message(a.asInstanceOf[AnyRef], time))
    output(1, Message(b.asInstanceOf[AnyRef], time))
  }
}

object Unzip2Task {
  case class UnZipFunction[In, A1, A2](unzip: In => (A1, A2)) extends Serializable

  val UNZIP2_FUNCTION = "org.apache.gearpump.akkastream.task.unzip2.function"
}
