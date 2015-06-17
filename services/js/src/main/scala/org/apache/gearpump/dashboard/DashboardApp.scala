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
import org.apache.gearpump.dashboard.directives._
import org.apache.gearpump.dashboard.filters.HumanizeFilter
import org.apache.gearpump.dashboard.filters.LastPartFilter
import org.apache.gearpump.dashboard.services._

import scala.scalajs.js.JSApp
import scala.scalajs.js.annotation.JSExport

@JSExport
object DashboardApp extends JSApp {
  override def main(): Unit = {
    println("DashboardApp")

    val conf = Angular.module("dashboard.conf", Array.empty[String])
    conf.factory[ConfServiceFactory]

    val tabset = Angular.module("directive.tabset", Array.empty[String])
    tabset.directive[TabSetDirective]
    tabset.directive[TabDirective]

    val explanationicon = Angular.module("directive.explanationicon", Array("mgcrea.ngStrap.tooltip"))
    explanationicon.directive[ExplanationIconDirective]

    val metricsCard = Angular.module("directive.metricscard", Array("directive.explanationicon"))
    metricsCard.filter[HumanizeFilter]
    metricsCard.directive[MetricsCardDirective]

    val restapi = Angular.module("dashboard.restapi", Array("dashboard.conf"))
    restapi.factory[RestApiServiceFactory]

    val cluster = Angular.module("dashboard.cluster", Array("ngRoute"))
    cluster.config[MasterConfig]
    cluster.controller[MasterCtrl]
    cluster.config[WorkersConfig]
    cluster.controller[WorkersCtrl]

    val apps = Angular.module("dashboard.apps", Array.empty[String])
    apps.config[AppsConfig]
    apps.controller[AppsCtrl]

    val visgraph = Angular.module("directive.visgraph", Array.empty[String])
    visgraph.directive[VisnetworkDirective]

    val appmaster = Angular.module("dashboard.apps.appmaster", Array("directive.visgraph"))
    appmaster.filter[LastPartFilter]
    appmaster.config[AppMasterConfig]
    appmaster.controller[AppMasterCtrl]
    appmaster.controller[AppStatusCtrl]
    appmaster.controller[AppSummaryChartsCtrl]
    appmaster.controller[AppDagCtrl]
    appmaster.controller[AppProcessorCtrl]
    appmaster.controller[AppMetricsCtrl]

    val module = Angular.module("dashboard", Array(
      "ngAnimate",
      "ngRoute",
      "ng-breadcrumbs",
      "mgcrea.ngStrap",
      "ui.select",
      "smart-table",
      "directive.tabset",
      "directive.explanationicon",
      "directive.metricscard",
      "dashboard.conf",
      "dashboard.restapi",
      "dashboard.cluster",
      "dashboard.apps",
      "dashboard.apps.appmaster"))
    module.config[DashboardConfig]
    module.factory[UtilServiceFactory]
    module.controller[DashboardCtrl]
  }
}


