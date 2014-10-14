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

package org.apache.gearpump.streaming.examples.kafka

import java.util.concurrent.TimeUnit

import akka.actor.Cancellable
import org.apache.gearpump.Message
import org.apache.gearpump.streaming.task.Handler.DefaultHandler
import org.apache.gearpump.streaming.task.{MessageHandler, TaskContext, TaskActor}
import org.apache.gearpump.util.Configs
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.HashMap
import scala.concurrent.duration.FiniteDuration

class Sum (conf : Configs) extends TaskActor(conf) with MessageHandler[String] {
  import org.apache.gearpump.streaming.examples.kafka.Sum._

  private val map : HashMap[String, Long] = new HashMap[String, Long]()

  private var wordCount : Long = 0
  private var snapShotTime : Long = System.currentTimeMillis()
  private var snapShotWordCount : Long = 0

  private var scheduler : Cancellable = null

  override def onStart(taskContext : TaskContext) : Unit = {
    import context.dispatcher
    scheduler = context.system.scheduler.schedule(new FiniteDuration(5, TimeUnit.SECONDS),
      new FiniteDuration(5, TimeUnit.SECONDS))(reportWordCount)
  }

  def onNext(msg: Message): Unit = {
    DefaultHandler
    doNext(msg)
  }

  def next(msg : String) : Unit = {
    if (null == msg) {
      return
    }
    val current = map.getOrElse(msg, 0L)
    wordCount += 1
    val word = msg
    val count = current + 1
    map.put(word, count)
    output(new Message(word -> count.toString(), System.currentTimeMillis()))
  }

  override def onStop() : Unit = {
    scheduler.cancel()
  }

  def reportWordCount : Unit = {
    val current : Long = System.currentTimeMillis()
    LOG.info(s"Task $taskId Throughput: ${((wordCount - snapShotWordCount), ((current - snapShotTime) / 1000))} (words, second)")
    snapShotWordCount = wordCount
    snapShotTime = current
  }
}

object Sum {
  private val LOG: Logger = LoggerFactory.getLogger(classOf[Sum])
}
