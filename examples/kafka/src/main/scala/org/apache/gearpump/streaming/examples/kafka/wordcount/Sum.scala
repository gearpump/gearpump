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

package org.apache.gearpump.streaming.examples.kafka.wordcount

import org.apache.gearpump.Message
import org.apache.gearpump.streaming.task.{TaskContext, TaskActor}
import org.apache.gearpump.util.{LogUtil, Configs}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

class Sum (conf : Configs) extends TaskActor(conf) {

  private val map : mutable.HashMap[String, Long] = new mutable.HashMap[String, Long]()

  override def onStart(taskContext : TaskContext) : Unit = {

  }

  override def onNext(msg : Message) : Unit = {
    if (null == msg) {
      return
    }
    val current = map.getOrElse(msg.msg.asInstanceOf[String], 0L)
    val word = msg.msg.asInstanceOf[String]
    val count = current + 1
    map.put(word, count)
    output(new Message(s"${msg.timestamp}" -> s"$word:$count", System.currentTimeMillis()))
  }

  override def onStop() : Unit = {
  }
}