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

import org.apache.gearpump.Message
import org.apache.gearpump.akkastream.task.Zip2Task.ZipFunction
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.streaming.task.TaskContext

class Zip2Task[A1, A2, OUT](context: TaskContext, userConf : UserConfig)
  extends GraphTask(context, userConf) {

  val zip = userConf.
    getValue[ZipFunction[A1, A2, OUT]](Zip2Task.ZIP2_FUNCTION)(context.system).get.zip
  var a1: Option[A1] = None
  var a2: Option[A2] = None

  override def onNext(msg: Message) : Unit = {
    val message = msg.value
    val time = msg.timestamp
    a1 match {
      case Some(x) =>
        a2 = Some(message.asInstanceOf[A2])
        a1.foreach(v1 => {
          a2.foreach(v2 => {
            val out = zip(v1, v2)
            context.output(Message(out.asInstanceOf[OUT], time))

          })
        })
      case None =>
        a1 = Some(message.asInstanceOf[A1])
    }
  }
}

object Zip2Task {
  case class ZipFunction[A1, A2, OUT](val zip: (A1, A2) => OUT) extends Serializable

  val ZIP2_FUNCTION = "org.apache.gearpump.akkastream.task.zip2.function"
}
