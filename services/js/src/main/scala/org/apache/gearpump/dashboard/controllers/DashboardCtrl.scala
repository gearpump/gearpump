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
import scala.scalajs.js.annotation.{JSExport, JSExportAll}

@JSExportAll
case class Link(label: String, url: String, iconClass: String)

@JSExport
@injectable("DashboardConfig")
class DashboardConfig(routeProvider: RouteProvider) extends Config {
  println("DashboardConfig")
  routeProvider.when ("/", Route.redirectTo ("/cluster") ).otherwise (Route.redirectTo ("/") )
}

trait DashboardScope extends Scope {
  var breadcrumbs: Service = js.native
  var links: js.Array[Link] = js.native
  var navClass: js.Function1[String, Boolean] = js.native
}

@JSExport
@injectable("DashboardCtrl")
class DashboardCtrl(scope: DashboardScope, location: Location)
  extends AbstractController[DashboardScope](scope) {

  println("DashboardCtrl")

  val links = js.Array(
    Link("Cluster", "#/cluster", "glyphicon glyphicon-th-large"),
    Link("Applications", "#/apps", "glyphicon glyphicon-tasks")
  )
  def navClass(url: String): Boolean = {
    val path = url.substring(1)
    val active = location.path().indexOf(path) == 0
    println(s"url=$url location=${location.path()} active=$active")
    active
  }

  scope.links = links
  scope.navClass = navClass _

 }

