package org.apache.gearpump.client
/**
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

import java.util
import java.util.concurrent.TimeUnit

import akka.actor.Cancellable
import org.apache.gearpump.task.TaskActor
import org.apache.gearpump.Partitioner
import org.slf4j.{LoggerFactory, Logger}

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.mutable.HashMap

class Sum extends TaskActor {
  import Sum._

  private val map : HashMap[String, Int] = new HashMap[String, Int]()
  private var scheduler : Cancellable = null

  override def preStart() : Unit = {
    scheduler = context.system.scheduler.schedule(new FiniteDuration(5, TimeUnit.SECONDS),
      new FiniteDuration(15, TimeUnit.SECONDS))(reportWordCount)
  }

  override def onNext(msg : String) : Unit = {
    if (null == msg) {
      return
    }
    val current = map.getOrElse(msg, 0)
    map.put(msg, current + 1)
  }

  override def onStop() : Unit = {
    scheduler.cancel()
  }

  def reportWordCount : Unit = {
    LOG.info("Reporting WordCount...")
    for (kv  <- map) {
      LOG.info(kv.toString())
    }
  }
}

object Sum {
  private val LOG: Logger = LoggerFactory.getLogger(Sum.getClass)
}
