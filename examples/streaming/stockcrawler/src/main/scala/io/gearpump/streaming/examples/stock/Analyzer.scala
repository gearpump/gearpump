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


package io.gearpump.streaming.examples.stock

import scala.collection.immutable

import akka.actor.Actor.Receive
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

import io.gearpump.Message
import io.gearpump.cluster.UserConfig
import io.gearpump.streaming.examples.stock.Analyzer.HistoricalStates
import io.gearpump.streaming.examples.stock.Price._
import io.gearpump.streaming.task.{StartTime, Task, TaskContext}
import io.gearpump.util.LogUtil

/**
 * Dradown analyzer
 * Definition: http://en.wikipedia.org/wiki/Drawdown_(economics)
 */
class Analyzer(taskContext: TaskContext, conf: UserConfig) extends Task(taskContext, conf) {

  val dateFormatter = DateTimeFormat forPattern "dd/MM/yyyy"

  private var stocksToReport = immutable.Set.empty[String]
  private var stockInfos = new immutable.HashMap[String, StockPrice]

  private var currentDownwardsStates = new immutable.HashMap[String, StockPriceState]
  private val historicalStates = new HistoricalStates()
  private var latestTimeStamp: Long = 0L

  override def onStart(startTime: StartTime): Unit = {
    LOG.info("analyzer is started")
  }

  override def onNext(msg: Message): Unit = {
    msg.msg match {
      case stock: StockPrice =>
        latestTimeStamp = stock.timestamp
        checkDate(stock)
        stockInfos += stock.stockId -> stock
        val downwardsState = updateCurrentStates(stock)
        val maxDrawdown = historicalStates.updatePresentMaximal(downwardsState)
    }
  }

  override def receiveUnManagedMessage: Receive = {
    case get@GetReport(stockId, date) =>
      var currentMax = currentDownwardsStates.get(stockId)

      val dateTime = Option(date) match {
        case Some(date) =>
          currentMax = None
          parseDate(dateFormatter, date)
        case None =>
          new DateTime(latestTimeStamp).withTimeAtStartOfDay
      }

      val historyMax = Option(dateTime).flatMap(handleHistoricalQuery(stockId, _))
      val name = stockInfos.get(stockId).map(_.name).getOrElse("")
      sender ! Report(stockId, name, dateTime.toString, historyMax, currentMax)
  }

  private def updateCurrentStates(stock: StockPrice) = {
    var downwardsState: StockPriceState = null
    if (currentDownwardsStates.contains(stock.stockId)) {
      downwardsState = generateNewState(stock, currentDownwardsStates.get(stock.stockId).get)
    } else {
      downwardsState = StockPriceState(stock.stockId, stock, stock, stock)
    }
    currentDownwardsStates += stock.stockId -> downwardsState
    downwardsState
  }

  // Update the stock's latest state.
  private def generateNewState(currentPrice: Price, oldState: StockPriceState): StockPriceState = {
    if (currentPrice.price > oldState.max.price) {
      StockPriceState(oldState.stockID, currentPrice, currentPrice, currentPrice)
    } else {
      val newState = StockPriceState(oldState.stockID, oldState.max,
        Price.min(currentPrice, oldState.min), currentPrice)
      newState
    }
  }

  private def checkDate(stock: StockPrice) = {
    if (currentDownwardsStates.contains(stock.stockId)) {
      val now = new DateTime(stock.timestamp)
      val lastTime = new DateTime(currentDownwardsStates.get(stock.stockId).get.current.timestamp)
      // New day
      if (now.getDayOfYear > lastTime.getDayOfYear || now.getYear > lastTime.getYear) {
        currentDownwardsStates -= stock.stockId
      }
    }
  }

  private def parseDate(format: DateTimeFormatter, input: String): DateTime = {
    format.parseDateTime(input)
  }

  private def handleHistoricalQuery(stockId: String, date: DateTime) = {
    val maximal = historicalStates.getHistoricalMaximal(stockId, date)
    maximal
  }
}

object Analyzer {

  class HistoricalStates {
    val LOG = LogUtil.getLogger(getClass)
    val dateFormatter = DateTimeFormat forPattern "dd/MM/yyyy"
    private var historicalMaxRaise = new immutable.HashMap[(String, DateTime), StockPriceState]
    private var historicalMaxDrawdown = new immutable.HashMap[(String, DateTime), StockPriceState]

    def updatePresentMaximal(newState: StockPriceState): Option[StockPriceState] = {
      val date = Analyzer.getDateFromTimeStamp(newState.current.timestamp)
      var newMaximalState: Option[StockPriceState] = null
      if (newState.max.price < Float.MinPositiveValue) {
        newMaximalState = generateNewMaximal(newState, date, historicalMaxRaise)
        if (newMaximalState.nonEmpty) {
          historicalMaxRaise += (newState.stockID, date) -> newMaximalState.get
        }
      } else {
        newMaximalState = generateNewMaximal(newState, date, historicalMaxDrawdown)
        if (newMaximalState.nonEmpty) {
          historicalMaxDrawdown += (newState.stockID, date) -> newMaximalState.get
        }
      }
      newMaximalState
    }

    def getHistoricalMaximal(stockId: String, date: DateTime): Option[StockPriceState] = {
      historicalMaxDrawdown.get((stockId, date))
    }

    private def generateNewMaximal(
        state: StockPriceState,
        date: DateTime,
        map: immutable.HashMap[(String, DateTime), StockPriceState])
      : Option[StockPriceState] = {
      val maximal = map.get((state.stockID, date))
      if (maximal.nonEmpty && maximal.get.drawDown > state.drawDown) {
        None
      } else {
        Some(state)
      }
    }
  }

  def getDateFromTimeStamp(timestamp: Long): DateTime = {
    new DateTime(timestamp).withTimeAtStartOfDay()
  }
}
