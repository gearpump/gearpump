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

package org.apache.gearpump.streaming.examples.sol

import java.util.concurrent.TimeUnit

import akka.actor.Cancellable
import org.apache.gearpump.Message
import org.apache.gearpump.streaming.task.TaskActor
import org.apache.gearpump.util.Configs
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.duration.FiniteDuration

class SOLBolt(conf : Configs) extends TaskActor(conf : Configs) {
  private val LOG: Logger = LoggerFactory.getLogger(classOf[SOLBolt])

  private var msgCount : Long = 0
  private var scheduler : Cancellable = null
  private var snapShotWordCount : Long = 0
  private var snapShotTime : Long = 0

  override def onStart() : Unit = {
    import context.dispatcher
    scheduler = context.system.scheduler.schedule(new FiniteDuration(5, TimeUnit.SECONDS),
      new FiniteDuration(5, TimeUnit.SECONDS))(reportWordCount)
    snapShotTime = System.currentTimeMillis()
  }

  override def onNext(msg : Message) : Unit = {
    output(msg)
    msgCount = msgCount + 1
  }

  override def onStop() : Unit = {
    scheduler.cancel()
  }

  def reportWordCount : Unit = {
    val current : Long = System.currentTimeMillis()
    LOG.info(s"Task $taskId Throughput: ${((msgCount - snapShotWordCount), ((current - snapShotTime) / 1000))} (words, second)")
    snapShotWordCount = msgCount
    snapShotTime = current
  }
}
