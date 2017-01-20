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
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.streaming.task.TaskContext

import scala.concurrent.Future

class MapAsyncTask[In, Out](context: TaskContext, userConf : UserConfig)
  extends GraphTask(context, userConf) {

  val f = userConf.getValue[In => Future[Out]](MapAsyncTask.MAPASYNC_FUNC)
  implicit val ec = context.system.dispatcher

  override def onNext(msg : Message) : Unit = {
    val data = msg.msg.asInstanceOf[In]
    val time = msg.timestamp
    f match {
      case Some(func) =>
        val fout = func(data)
        fout.onComplete(value => {
          value.foreach(out => {
            val msg = new Message(out, time)
            context.output(msg)
          })
        })
      case None =>
    }
  }
}

object MapAsyncTask {
  val MAPASYNC_FUNC = "MAPASYNC_FUNC"

}
