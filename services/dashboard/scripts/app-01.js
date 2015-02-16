/* *
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
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
'use strict';

angular.module('app-01', ['ngTable', 'readable'])
  .controller('app01Ctrl', function ($scope) {
    $scope.panes = [
      {name: "Master", templateUrl: "partials/master.html", controller: "masterControl"},
      {name: "Workers", templateUrl: "partials/workers.html", controller: "workerControl"},
    ];
  })
  .controller('masterControl', function ($scope, $http) {
    var url = location.origin + '/master';
    $scope.summary = {};
    $http.get(url).then(function (response) {
      var description = response.data.masterDescription;
      $scope.summary = {
        aliveFor: description.aliveFor,
        homeDir: description.homeDirectory,
        logFile: description.logFile,
        jarStore: description.jarStore,
        status: description.masterStatus,
        leader: description.leader.join(':'),
        clusters: description.cluster.map(function(item) {
            return item.join(':');
          })
      };
    }, function (err) {
      throw err;
    });
  })
  .controller('workerControl', function ($scope, $http, $filter, ngTableParams) {
    $scope.tableParams = new ngTableParams({page: 1, count: 10}, {
      counts: [10, 25, 50],
      total: 0,
      getData: function ($defer, params) {
        var url = location.origin + '/workers';
        var workers = [];
        $http.get(url).then(function (response) {
          workers = response.data.map(function(worker) {
            return {
              id: worker.workerId,
              state: worker.state,
              actorPath: worker.actorPath,
              aliveFor: worker.aliveFor,
              homeDir: worker.homeDirectory,
              logFile: worker.logFile,
              executors: worker.executors,
              slotsTotal: worker.totalSlots,
              slotsTaken: worker.totalSlots - worker.availableSlots,
              slotUsage: worker.totalSlots > 0 ?
                Math.floor(100 * (worker.totalSlots - worker.availableSlots) / worker.totalSlots) : 0
            }
          });
          var orderedData = params.sorting() ?
              $filter('orderBy')(workers, $scope.tableParams.orderBy()) : workers;
          $defer.resolve(
              orderedData.slice((params.page() - 1) * params.count(),
                  params.page() * params.count()));
        }, function (err) {
          throw err;
        });
      }
    });
  });
