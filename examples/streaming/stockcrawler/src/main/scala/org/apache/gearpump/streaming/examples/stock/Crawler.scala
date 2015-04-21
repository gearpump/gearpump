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


package org.apache.gearpump.streaming.examples.stock

import org.apache.gearpump.Message
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.streaming.task.{StartTime, Task, TaskContext}

import scala.concurrent.duration._

class Crawler(taskContext: TaskContext, conf: UserConfig) extends Task(taskContext, conf) {

  import taskContext._

  val FetchStockPrice = Message("FetchStockPrice")

  lazy val stocks = {
    val stockIds = conf.getValue[Array[String]]("StockId").get
    val size = if (stockIds.length % parallelism > 0) {
      stockIds.length / parallelism + 1
    } else {
      stockIds.length / parallelism
    }

    val start = taskId.index * size
    val end = (taskId.index + 1) * size
    stockIds.slice(start, end)
  }

  scheduleOnce(1.seconds)(self ! FetchStockPrice)

  val stockMarket = conf.getValue[StockMarket](classOf[StockMarket].getName).get

  override def onStart(startTime : StartTime) : Unit = {
    //nothing
  }

  override def onNext(msg : Message) : Unit = {
    stockMarket.getPrice(stocks).foreach {price =>
      output(new Message(price, price.timestamp))
    }
    scheduleOnce(5.seconds)(self ! FetchStockPrice)
  }
}
