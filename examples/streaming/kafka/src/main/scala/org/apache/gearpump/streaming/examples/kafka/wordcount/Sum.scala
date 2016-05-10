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

package org.apache.gearpump.streaming.examples.kafka.wordcount

import com.twitter.bijection.Injection

import org.apache.gearpump.Message
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.streaming.task.{StartTime, Task, TaskContext}

class Sum(taskContext: TaskContext, conf: UserConfig) extends Task(taskContext, conf) {
  import taskContext.output

  private[wordcount] var wordcount = Map.empty[String, Long]

  override def onStart(startTime: StartTime): Unit = {}

  override def onNext(message: Message): Unit = {
    val word = message.msg.asInstanceOf[String]
    val count = wordcount.getOrElse(word, 0L) + 1
    wordcount += word -> count
    output(new Message(
      Injection[String, Array[Byte]](word) ->
        Injection[Long, Array[Byte]](count),
      message.timestamp))
  }
}