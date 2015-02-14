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

angular.module('app.widgets.visdag', ['adf.provider'])
.value('appMasterUrl', location.href)
.config(function (dashboardProvider) {
  dashboardProvider.widget('visdag', {
    title: 'Dag',
    description: 'Dag widget',
    controller: 'visDagCtrl',
    templateUrl: 'scripts/widgets/visdag/visdag.html',
    appId: -1,
    edit: {
      templateUrl: 'scripts/widgets/visdag/edit.html',
      reload: false
    }
  });
})
.controller('visDagCtrl', function ($scope, $http) {
  $scope.$on('appmaster-selected', function (event, appMasterSelected) {
    var url = location.origin + '/appmaster/' + appMasterSelected.appId + '?detail=true';
    $http.get(url).then(function (response) {
      var json = response.data;
      $scope.data = {
        nodes: [],
        edges: []
      };

      function lastPart(name) {
        var parts = name.split(/\./);
        return parts[parts.length - 1];
      }

      json.dag.vertices.forEach(function (vertex, i) {
        var name = lastPart(vertex);
        $scope.data.nodes.push({id: name, label: name});
      });

      json.dag.edges.forEach(function (edge, i) {
        var source = lastPart(edge[0]);
        var target = lastPart(edge[2]);
        var value = lastPart(edge[1]);
        $scope.data.edges.push({from: source, to: target, label: value});
      });

      $scope.$broadcast('appmaster-data', {
        data: $scope.data
      });
    }, function (err) {
      throw err;
    });
  });
})
.directive('visdag', function () {
  function visdag(scope, el, attr) {
    var data = {
      nodes: new vis.DataSet(),
      edges: new vis.DataSet()
    };
    var options = {
      width: '100%',
      height: '700px',
      hierarchicalLayout: {
        layout: 'direction'
      },
      stabilize: true /* stabilize positions before displaying */,
      nodes: {
        radiusMin: 16,
        radiusMax: 24
      },
      edges: {
        style: 'arrow',
        labelAlignment: 'line-center',
        fontSize: 12
      }
    };
    new vis.Network(el[0], data, options);

    scope.$on('appmaster-data', function (event, newVal) {
      data.nodes.update(newVal.data.nodes);
      data.edges.update(newVal.data.edges);
    });
  }

  return {link: visdag, restrict: 'E', scope: {data: '='}};
});
