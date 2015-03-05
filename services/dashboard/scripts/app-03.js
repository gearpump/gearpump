/* *
 * The MIT License
 * 
 * Copyright (c) 2014, Sebastian Sdorra
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
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
'use strict';

angular.module('app-03', [])
  .controller('app03Ctrl', function ($scope, $http, $routeParams) {
    var url = location.origin + '/appmaster/' + $routeParams.appId + '?detail=true';
    $http.get(url).then(function (response) {
        var data = response.data;
        if (data.hasOwnProperty("appName")) {
          $scope.app = {
            actorPath: data.actorPath,
            duration: data.clock,
            executors: data.executors,
            id: data.appId,
            name: data.appName
          };
        }
        if (data.hasOwnProperty('dag') && data.hasOwnProperty('processors')) {
          if (!$scope.dag) {
            initDag();
          }
          updateDag(data.dag, data.processors);
        }
      },
      function (err) {
        console.log(err);
        throw err;
      });

    function initDag() {
      $scope.dag = {
        options: {
          width: '100%',
          height: '100%',
          hierarchicalLayout: {
            layout: 'direction',
            direction: "UD"
          },
          stabilize: true /* stabilize positions before displaying */,
          freezeForStabilization: true,
          nodes: {},
          edges: {
            style: 'arrow',
            labelAlignment: 'line-center',
            fontSize: 12
          }
        },
        data: {
          nodes: new vis.DataSet(),
          edges: new vis.DataSet()
        }
      };
    }

    function updateDag(dag, processors) {

      function lastPart(name) {
        var parts = name.split('.');
        return parts[parts.length - 1];
      }

      var processorLookup = {};
      processors.map(function (item) {
        processorLookup[item[0]] = item[1];
      });

      var updates = {
        nodes: [],
        edges: []
      };

      dag.vertices.forEach(function (id, i) {
        var name = lastPart(processorLookup[id].taskClass);
        var item = $scope.dag.data.nodes.get(id);
        if (!item || item.label !== name) {
          updates.nodes.push({id: id, label: name});
        }
      });
      dag.edges.forEach(function (edge, i) {
        var source = edge[0];
        var target = edge[2];
        var value = lastPart(edge[1]);
        var id = source + "_" + target;
        var item = $scope.dag.data.edges.get(id);
        if (!item || item.label !== value) {
          updates.edges.push({id: id, from: source, to: target, label: value});
        }
      });

      $scope.dag.data.nodes.update(updates.nodes);
      $scope.dag.data.edges.update(updates.edges);
    }
  })
;
