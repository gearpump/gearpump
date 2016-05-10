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

package org.apache.gearpump.streaming.examples.stock

// scalastyle:off equals.hash.code  case class has equals defined
case class StockPrice(
    stockId: String, name: String, price: String, delta: String, pecent: String, volume: String,
    money: String, timestamp: Long) {
  override def hashCode: Int = stockId.hashCode
}
// scalastyle:on equals.hash.code  case class has equals defined

case class Price(price: Float, timestamp: Long)

object Price {

  import scala.language.implicitConversions

  implicit def StockPriceToPrice(stock: StockPrice): Price = {
    Price(stock.price.toFloat, stock.timestamp)
  }

  def min(first: Price, second: Price): Price = {
    if (first.price < second.price) {
      first
    } else {
      second
    }
  }
}

case class StockPriceState(stockID: String, max: Price, min: Price, current: Price) {

  def drawDownPeriod: Long = min.timestamp - max.timestamp

  def recoveryPeriod: Long = current.timestamp - min.timestamp

  def drawDown: Float = max.price - min.price
}

case class GetReport(stockId: String, date: String)

case class Report(
    stockId: String, name: String, date: String, historyMax: Option[StockPriceState],
    currentMax: Option[StockPriceState])
