/*
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
angular.module('dashboard')

  .directive('metricSelector', function () {
    'use strict';

    return {
      restrict: 'E',
      templateUrl: 'views/apps/streamingapp/metric_selector.html',
      scope: {
        metricName: '=',
        metricType: '=',
        metricDescription: '=?'
      },
      link: function (scope) {
        'use strict';

        var metricNameLookup = {
          'Message Send Throughput': {type: 'meter', name: 'sendThroughput'},
          'Message Receive Throughput': {type: 'meter', name: 'receiveThroughput'},
          'Average Message Processing Time': {type: 'histogram', name: 'processTime'},
          'Average Message Receive Latency': {type: 'histogram', name: 'receiveLatency'}
        };

        scope.names = {available: _.keys(metricNameLookup)};
        scope.names.selected = scope.names.available[1]; // use Receive Throughput by default
        scope.types = function (name) {
          return metricNameLookup[name].type;
        };

        scope.$watch('names.selected', function (val) {
          scope.metricName = metricNameLookup[val].name;
          scope.metricType = metricNameLookup[val].type;
          scope.metricDescription = val;
        });
      }
    }
  })
;
