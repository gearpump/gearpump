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

package org.apache.gearpump.dashboard

import com.greencatsoft.angularjs.Angular
import org.apache.gearpump.dashboard.controllers._
import org.apache.gearpump.dashboard.directives.ExplanationIconDirective
import org.apache.gearpump.dashboard.services.{OptionsServiceFactory, RestApiServiceFactory}

import scala.scalajs.js.JSApp
import scala.scalajs.js.annotation.JSExport

@JSExport
object DashboardApp extends JSApp {
  override def main(): Unit = {
    println("DashboardApp")

    val options = Angular.module("dashboard.options", Array.empty[String])
    options.factory[OptionsServiceFactory]

    val explanationicon = Angular.module("directive.explanationicon", Array("mgcrea.ngStrap.tooltip"))
    explanationicon.directive[ExplanationIconDirective]

    val restapi = Angular.module("dashboard.restapi", Array("dashboard.options"))
    restapi.factory[RestApiServiceFactory]

    val cluster = Angular.module("dashboard.cluster", Array("ngRoute"))
    cluster.config[MasterConfig]
    cluster.controller[MasterCtrl]
    cluster.config[WorkersConfig]
    cluster.controller[WorkersCtrl]

    val apps = Angular.module("dashboard.apps", Array.empty[String])
    apps.config[AppsConfig]
    apps.controller[AppsCtrl]

    val module = Angular.module("dashboard", Array(
      "ngAnimate",
      "ngRoute",
      "ng-breadcrumbs",
      "mgcrea.ngStrap",
      "ui.select",
      "smart-table",
      "directive.explanationicon",
      "dashboard.options",
      "dashboard.restapi",
      "dashboard.cluster",
      "dashboard.apps"))

    module.config[DashboardConfig]
    module.controller[DashboardCtrl]
    module.controller[AppDagCtrl]

  }
}


