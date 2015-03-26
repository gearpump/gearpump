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

import java.nio.charset.Charset
import org.apache.commons.httpclient.methods.GetMethod
import org.apache.commons.httpclient.{HttpClient, MultiThreadedHttpConnectionManager}
import org.apache.gearpump.streaming.examples.stock.StockMarket.ServiceHour
import org.apache.gearpump.transport.HostPort
import org.apache.gearpump.util.LogUtil
import org.htmlcleaner.{HtmlCleaner, TagNode}
import org.joda.time.{DateTime, DateTimeZone}

import scala.io.Codec

class StockMarket(service: ServiceHour, proxy: HostPort = null) extends Serializable {

  def LOG = LogUtil.getLogger(getClass)

  @transient
  private var connectionManager: MultiThreadedHttpConnectionManager = null

  private val eastMoneyStockPage = "http://quote.eastmoney.com/stocklist.html"

  private val stockPriceParser =
    """^var\shq_str_s_([a-z0-9A-Z]+)="([^,]+),([^,]+),([^,]+),([^,]+),([^,]+),([^,]+)";$""".r

  def shutdown: Unit = {
    Option(connectionManager).map(_.shutdown())
  }

  @transient
  private var _client: HttpClient = null

  private def client: HttpClient = {
    _client = Option(_client).getOrElse {
      val connectionManager = new MultiThreadedHttpConnectionManager()
      val client = new HttpClient(connectionManager)
      Option(proxy).map(host => client.getHostConfiguration().setProxy(host.host, host.port))
      client
    }
    _client
  }

  def getPrice(stocks: Array[String]) : Array[StockPrice] = {


    LOG.info(s"getPrice 1")

    val query = "http://hq.sinajs.cn/list=" + stocks.map("s_" + _).mkString(",")
    if (service.inService) {

      LOG.info(s"getPrice 2")

      val get = new GetMethod(query)
      client.executeMethod(get)
      val current = System.currentTimeMillis()

      val output = scala.io.Source.fromInputStream(get.getResponseBodyAsStream)(new Codec(Charset forName "GBK")).getLines().flatMap { line =>
        line match {
          case stockPriceParser(stockId, name, price, delta, pecent, volume, money) =>
            Some(StockPrice(stockId, name, price, delta, pecent, volume, money, current))
          case _ =>
            None
        }
      }.toArray

      LOG.info(s"getPrice 3 ${output.length}")

      output
    } else {
      Array.empty[StockPrice]
    }
  }

  private val urlPattern = """^.*/([a-zA-Z0-9]+)\.html$""".r

  def getStockIdList: Array[String] = {
    val cleaner = new HtmlCleaner
    val props = cleaner.getProperties

    val get = new GetMethod(eastMoneyStockPage)
    client.executeMethod(get)

    val root = cleaner.clean(get.getResponseBodyAsStream)

    val stockUrls = root.evaluateXPath("//div[@id='quotesearch']//li//a[@href]");

    val elements = root.getElementsByName("a", true)

    val hrefs = (0 until stockUrls.length)
      .map(stockUrls(_).asInstanceOf[TagNode].getAttributeByName("href"))
      .map {url =>
      url match {
        case urlPattern(code) => code
        case _ => null
      }
    }.toArray
    hrefs
  }
}

object StockMarket {

  class ServiceHour(all: Boolean) extends Serializable {

    /**
     * Morning openning: 9:30 am - 11:30 am
     */
    val morningStart = GMT8(new DateTime(0,1,1,9,30)).getMillis
    val morningEnd = GMT8(new DateTime(0,1,1,11,30)).getMillis

    /**
     * After noon openning: 13:00 pm - 15:00 pm
     */
    val afternoonStart = GMT8(new DateTime(0,1,1,13,0)).getMillis
    val afternoonEnd = GMT8(new DateTime(0,1,1,15,0)).getMillis

    def inService: Boolean = {

      if (all) {
        true
      } else {
        val now = GMT8(DateTime.now()).withDate(0, 1, 1).getMillis
        if (now >= morningStart && now <= morningEnd ||
          now >= afternoonStart && now <= afternoonEnd) {
          true
        } else {
          false
        }
      }
    }

    private def GMT8(time: DateTime): DateTime = {
      time.withZone(DateTimeZone.UTC).plusHours(8)
    }
  }
}
