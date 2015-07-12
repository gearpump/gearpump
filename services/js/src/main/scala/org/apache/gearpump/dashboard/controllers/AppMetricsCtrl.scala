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

package org.apache.gearpump.dashboard.controllers

import com.greencatsoft.angularjs.{AbstractController, injectable}
import org.apache.gearpump.dashboard.services.ConfService
import org.apache.gearpump.shared.Messages.{MetricInfo, MetricType}

import scala.scalajs.js
import scala.scalajs.js.annotation.JSExport


@JSExport
@injectable("AppMetricsCtrl")
class AppMetricsCtrl(scope: AppMasterScope, conf: ConfService) extends AbstractController[AppMasterScope](scope) {
  import js.JSConverters._

  val lookup = Map[String, String](
    "receivedMessages" -> "Receive Throughput",
    "sentMessages" -> "Send Throughput",
    "processingTime" -> "Processing Time",
    "receiveLatency" -> "Receive Latency"
  )
  var watchFn: js.Function = _

  def init(): Unit = {
    scope.itemsByPage = 15
    scope.taskName = metricInfo _
    val available = lookup.values.toSeq.toJSArray
    val selected = available(0)
    scope.names = js.Dynamic.literal(
      available=available,
      selected=selected
    ).asInstanceOf[MetricNames]
    scope.isMeter = isMeter _
  }

  def metricInfo(metrics: MetricInfo[MetricType]): String = {
     s"processor${metrics.processorId}.task${metrics.taskId}"
  }

  def getMetricsClassByLabel(label: String): Option[String] = {
    scope.names.available.indexOf(label) match {
      case -1 =>
        None
      case i =>
        Some(lookup.keys.toSeq.toJSArray(i))
    }
  }

  def isMeter(): Boolean = {
    getMetricsClassByLabel(scope.names.selected) match {
      case None =>
        false
      case Some(label) =>
        label match {
          case "receivedMessages" =>
            true
          case "sentMessages" =>
            true
          case "processingTime" =>
            false
          case "receiveLatency" =>
            false
          case _ =>
            false
        }
    }
  }

  import scalajs.js.timers._

  @JSExport
  def refreshMetrics(clazz: String)(): Unit = {
    println("refreshMetrics")
    val streamingDag = scope.streamingDag.get
    clazz match {
      case "receivedMessages" =>
        scope.metrics = streamingDag.meter("receiveThroughput").values.toSeq.toJSArray
      case "sentMessages" =>
        scope.metrics = streamingDag.meter("sendThroughput").values.toSeq.toJSArray
      case "processingTime" =>
        scope.metrics = streamingDag.histogram("processTime").values.toSeq.toJSArray
      case "receiveLatency" =>
        scope.metrics = streamingDag.histogram("receiveLatency").values.toSeq.toJSArray
      case unknown =>
        println("unknown type $unknown")
        scope.metrics = js.Array[MetricInfo[_<:MetricType]]()
    }
    /*
    val selected = getMetricsClassByLabel(scope.names.selected).getOrElse("")
    clazz.eq(selected) match {
      case true =>
        setTimeout(conf.conf.updateMetricsInterval)(refreshMetrics(clazz))
      case false =>
    }
    */
  }

  scope.$watch("streamingDag", init _)
  scope.$watch("names.selected", (newVal: String) => {
    getMetricsClassByLabel(newVal) match {
      case None =>
        scope.metrics = js.Array[MetricInfo[_<:MetricType]]()
      case Some(clazz) =>
        println(s"watching $clazz")
        scope.$watch(clazz, refreshMetrics(clazz) _)
    }
  }: Unit)
}

