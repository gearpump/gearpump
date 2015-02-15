/**
 * The MIT License
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
'use strict';

angular.module("app.tabset", [])
.directive("tabs", function () {
  return {
    restrict: "E",
    transclude: true,
    scope: {},
    controller: function($scope, $element) {
      var panes = $scope.panes = [];
      $scope.select = function(pane) {
        panes.forEach(function(pane) {
          pane.selected = false;
        });
        if (pane.load !== undefined) {
          pane.load();
        }
        pane.selected = true;
      };

      this.addPane = function (pane) {
        if (panes.length === 0) $scope.select(pane);
        panes.push(pane);
      };
    },
    template:
      '<div>' +
        '<ul class="nav nav-tabs nav-tabs-google">' +
          '<li ng-repeat="pane in panes" ng-class="{active:pane.selected}">' +
            '<a href="" ng-click="select(pane)">{{pane.title}}</a>' +
          '</li>' +
        '</ul>' +
        '<div class="tab-content" ng-transclude></div>' +
      '</div>',
    replace: true
  };
})
.directive("pane", ["$http", "$templateCache", "$controller", "$compile",
    function($http, $templateCache, $controller, $compile) {
  return {
    require: "^tabs",
    restrict: "E",
    transclude: true,
    scope: {title: "@"},
    link: function(scope, elem, attrs, tabs) {
      var templateCtrl, templateScope;
      if (attrs.templateUrl && attrs.controller) {
        scope.load = function() {
          $http.get(attrs.templateUrl, {cache: $templateCache})
            .then(function(response) {
              templateScope = scope.$new();
              templateCtrl = $controller(attrs.controller, {$scope: templateScope});
              elem.html(response.data);
              elem.children().data('$ngControllerController', templateCtrl);
              $compile(elem.contents())(templateScope);
            });
        };
      }
      tabs.addPane(scope);
    },
    template: '<div class="tab-pane" ng-class="{active: selected}" ng-transclude></div>',
    replace: true
  };
}]);