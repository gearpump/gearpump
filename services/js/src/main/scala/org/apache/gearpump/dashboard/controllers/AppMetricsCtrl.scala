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
import org.apache.gearpump.shared.Messages.{MetricType, MetricInfo}

import scala.scalajs.js
import scala.scalajs.js.UndefOr
import scala.scalajs.js.annotation.JSExport


@JSExport
@injectable("AppMetricsCtrl")
class AppMetricsCtrl(scope: AppMasterScope)
  extends AbstractController[AppMasterScope](scope) {

  println("AppMetricsCtrl")

  val lookup = Map[String, String](
    "streamingDag.meter.receiveThroughput" -> "Receive Throughput",
    "streamingDag.meter.sendThroughput" -> "Send Throughput",
    "streamingDag.histogram.processTime" -> "Processing Time",
    "streamingDag.histogram.receiveLatency" -> "Receive Latency"
  )

  import js.JSConverters._

  def init(): Unit = {
    scope.itemsByPage = 15
    scope.taskName = metricInfo _
    scope.names = js.Dynamic.literal(
      available=lookup.values.toSeq.toJSArray,
      selected=lookup.values.head
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
        label.indexOf(".meter.") > 0
    }
  }

  scope.$watch("streamingDag", init _)

  var watchFn: js.Function = _
  scope.$watch("names.selected", (newVal: String) => {
    if (watchFn != null) {
      val fn = watchFn.asInstanceOf[js.Function0[Unit]]
      fn()
      watchFn = null
    }
    getMetricsClassByLabel(newVal) match {
      case None =>
        scope.metrics = js.Array[String]()
      case Some(clazz) =>
        watchFn = scope.$watchCollection(clazz, (array: UndefOr[Map[String,String]]) => {
          array.toOption.map(array => {
            scope.metrics = array.values.toSeq.toJSArray
          })
        })
    }
  }: Unit)
}
