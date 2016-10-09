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
package org.apache.gearpump.streaming.dsl.window.impl

import java.time.Instant

import akka.actor.ActorSystem
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.streaming.Constants._
import org.apache.gearpump.streaming.Processor
import org.apache.gearpump.{Message, TimeStamp}
import org.apache.gearpump.streaming.dsl.window.api._
import org.apache.gearpump.streaming.dsl.task.{CountTriggerTask, EventTimeTriggerTask, ProcessingTimeTriggerTask}
import org.apache.gearpump.streaming.task.Task

object Bucket {
  def ofEpochMilli(startTime: TimeStamp, endTime: TimeStamp): Bucket = {
    Bucket(Instant.ofEpochMilli(startTime), Instant.ofEpochMilli(endTime))
  }
}

/**
 * A window unit including startTime and excluding endTime.
 */
case class Bucket(startTime: Instant, endTime: Instant) extends Comparable[Bucket] {
  override def compareTo(o: Bucket): Int = {
    val ret = startTime.compareTo(o.startTime)
    if (ret != 0) {
      ret
    } else {
      endTime.compareTo(o.endTime)
    }
  }
}

case class GroupAlsoByWindow[T, GROUP](groupByFn: T => GROUP, window: Window)
  extends GroupByFn[T, (GROUP, List[Bucket])] {

  override def groupBy(message: Message): (GROUP, List[Bucket]) = {
    val group = groupByFn(message.msg.asInstanceOf[T])
    val buckets = window.windowFn(Instant.ofEpochMilli(message.timestamp))
    group -> buckets
  }

  override def getProcessor(parallelism: Int, description: String,
      userConfig: UserConfig)(implicit system: ActorSystem): Processor[_ <: Task] = {
    val config = userConfig.withValue(GEARPUMP_STREAMING_GROUPBY_FUNCTION, this)
    window.trigger match {
      case CountTrigger =>
        Processor[CountTriggerTask[T, GROUP]](parallelism, description, config)
      case ProcessingTimeTrigger =>
        Processor[ProcessingTimeTriggerTask[T, GROUP]](parallelism, description, config)
      case EventTimeTrigger =>
        Processor[EventTimeTriggerTask[T, GROUP]](parallelism, description, config)
    }
  }

}


