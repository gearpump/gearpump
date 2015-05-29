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

import com.greencatsoft.angularjs._
import com.greencatsoft.angularjs.core.{Location, Route, RouteProvider, Scope}

import scala.scalajs.js
import scala.scalajs.js.annotation.JSExport

case class Link(label: String, url: String, iconClass: String)
case class Conf(updateChartInterval: Int, updateVisDagInterval: Int,
                restapiAutoRefreshInterval: Int, restapiRoot: String,
                webSocketPreferred: Boolean, webSocketSendTimeout: Int)

trait DashboardScope extends Scope {

  var breadcrumbs: Service = js.native

  var links: Array[Link] = js.native

  var navClass: (String) => Boolean = js.native

  var conf: Conf = js.native

}

object DashboardRoutingConfig extends Config {

  @inject
  var routeProvider: RouteProvider = _

  override def initialize() {
    routeProvider
      .when("/", Route.redirectTo("/cluster")).otherwise(Route.redirectTo("/"))
  }

}

@JSExport
@injectable("dashboardCtrl")
class DashboardCtrl(scope: DashboardScope, location: Location, breadcrumbs: Service)
  extends AbstractController[DashboardScope](scope) {

  scope.breadcrumbs = breadcrumbs
  scope.links = Array(
    Link("Cluster", "#/cluster", "glyphicon glyphicon-th-large"),
    Link("Applications", "#/apps", "glyphicon glyphicon-tasks")
  )
  scope.navClass = (url) => {
    val path = url.substring(1)
    location.path().indexOf(path) == 0
  }
  scope.conf = Conf(2000, 2000, 2000, location.path() + "api/v1.0", false, 500)
}

