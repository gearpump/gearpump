package org.apache.gearpump.dashboard.controllers

import com.greencatsoft.angularjs.core.Scope
import com.greencatsoft.angularjs.{AbstractController, injectable}

import scala.scalajs.js
import scala.scalajs.js.annotation.JSExport


@JSExport
@injectable("TabSetCtrl")
class TabSetCtrl(scope: AppMasterScope)
  extends AbstractController[AppMasterScope](scope) {

  println("TabSetCtrl")

  def apply(scope: AppMasterScope): TabSetCtrl = {
    this(scope)
  }

  @JSExport
  def addTab(tab: Tab): Unit = {
    scope.tabs.push(tab)
    if(scope.tabs.length == 1) {
      //scope.selectTab(tab, false)
    }
  }

  @JSExport
  def selectTab(selectedTab: Tab, reload: Boolean): Unit = {
    println("selectTab")
    scope.tabs.foreach(tab => {
      selectedTab == tab match {
        case true =>
          //scope.selectedTab = tab
        case false =>
      }
    })
  }



  /*
          var tabs = $scope.tabs = [];
        $scope.selectTab = function (tab, reload) {
          angular.forEach(tabs, function (item) {
            item.selected = item === tab;
          });
          if (tab.load !== undefined) {
            tab.load(reload);
          }
        };
        this.addTab = function (tab) {
          tabs.push(tab);
          if (tabs.length === 1) {
            $scope.selectTab(tab);
          }
        };
        $scope.$watch('switchTo', function (args) {
          if (args) {
            var tabIndex = args.tabIndex;
            if (tabIndex >= 0 && tabIndex < tabs.length) {
              $scope.selectTab(tabs[tabIndex], args.reload);
            }
            $scope.switchTo = null;
          }
        });
   */



}
