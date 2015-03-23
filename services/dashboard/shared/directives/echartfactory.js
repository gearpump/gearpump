/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */
'use strict';

angular.module('directive.echartfactory', [])

/** Directive of line chart */
  .directive('linechart', [function () {
    return {
      restrict: 'EA',
      transclude: false,
      scope: {
        options: '=',
        data: '='
      },
      link: function (scope, elem) {
        var steps = scope.options.steps || 25;
        var option = {
          tooltip: {
            trigger: 'axis',
            textStyle: {fontSize: 12},
            axisPointer: {type: 'none'}
          },
          dataZoom: {show: false},
          grid: {borderWidth: 0, y: 10, x2: 30, y2: 24},
          xAxis: [
            {
              type: 'category',
              boundaryGap: false,
              splitLine: {show: false},
              axisLine: {lineStyle: {width: 1}},
              data: (function () {
                var now = new Date();
                var res = [];
                var len = steps;
                while (len--) {
                  res.unshift(dateAxisLabel(now));
                  now = new Date(now - 2000);
                }
                return res;
              })()
            }
          ],
          yAxis: [
            {
              type: 'value',
              splitNumber: 3,
              splitLine: {show: false},
              axisLine: {show: false}
            }
          ],
          series: [
            {
              name: 'Throughput',
              type: 'line',
              data: (function () {
                var res = [];
                var len = steps;
                while (len--) {
                  res.push(undefined);
                }
                return res;
              })(),
              smooth: true,
              itemStyle: {
                normal: {
                  areaStyle: {type: 'default', color: 'rgb(229,242,250)'},
                  lineStyle: {color: 'rgb(0,119,215)', width: 3},
                  color: 'transparent'
                },
                emphasis: {
                  color: 'rgb(0,119,215)'
                }
              }
            }
          ],
          addDataAnimation: false
        };

        if (scope.options.height) {
          elem[0].style.height = scope.options.height;
        }
        var chart = echarts.init(elem[0]);
        chart.setOption(option, true);

        scope.$watch('data', function (array) {
          var data = [];
          angular.forEach(array, function (value, i) {
            data.push([i, value, false, false, dateAxisLabel(new Date())]);
          });
          chart.addData(data);
        });

        function dateAxisLabel(d) {
          return moment(d).format('HH:mm:ss');
        }
      }
    };
  }])
;
