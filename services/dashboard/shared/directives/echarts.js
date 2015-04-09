/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */
'use strict';

angular.module('directive.echarts', [])

/**
 * The angular directive of echarts control.
 */
  .directive('echart', [function () {
    return {
      restrict: 'A',
      scope: {options: '=', data: '='},
      link: function (scope, elems) {
        var options = scope.options;
        var elem0 = elems[0];
        elem0.style.width = options.width;
        elem0.style.height = options.height;
        var chart = echarts.init(elem0);
        chart.setOption(options, /*noMerge=*/true);

        scope.$watch('data', function (data) {
          // Expected to be an array of data series. E.g.: {x: new Date(), y: [400, 300]}
          if (data) {
            var dataGrow = !options.xAxisDataNum ||
              chart.getOption().xAxis[0].data.length < options.xAxisDataNum;
            if (!Array.isArray(data)) {
              data = [data];
            }
            var array = [];
            angular.forEach(data, function (moment) {
              angular.forEach(moment.y, function (value, i) {
                var params = [i, value, /*isHead=*/false, dataGrow];
                if (i === 0) {
                  params.push(moment.x);
                }
                array.push(params);
              });
            });
            chart.addData(array);
          }
        });
      }
    };
  }])

/**
 * Sparkline is an one data series line chart without axis labels.
 */
  .directive('sparkline', [function () {
    return {
      restrict: 'E',
      scope: {options: '=', data: '='},
      controller: ['$scope', 'LookAndFeel', function ($scope, LookAndFeel) {
        var use = $scope.options;
        var colors = LookAndFeel.colorSet(0);
        var options = {
          tooltip: LookAndFeel.tooltip({color: colors.grid}),
          dataZoom: {show: false},
          grid: {borderWidth: 0, x: 5, y: 5, x2: 5, y2: 0},
          xAxis: [{
            axisLabel: false,
            splitLine: false,
            data: use.xAxisData
          }],
          yAxis: [{show: false}],
          series: [LookAndFeel.makeDataSeries(
            angular.merge({colors: colors}, use.series[0]))]
        };
        $scope.options = angular.merge(options, use.inject);
      }],
      template: '<div echart options="options" data="data"></div>'
    };
  }])

/**
 * Line chart control.
 */
  .directive('linechart1', [function () {
    return {
      restrict: 'E',
      scope: {options: '=', data: '='},
      controller: ['$scope', 'LookAndFeel', function ($scope, LookAndFeel) {
        var use = $scope.options;
        var borderLineStyle = {lineStyle: {width: 1, color: '#ccc'}};
        var options = {
          tooltip: LookAndFeel.tooltip({}),
          dataZoom: {show: false},
          grid: {borderWidth: 0, y: 10, x2: 30, y2: 20},
          xAxis: [{
            axisLabel: {show: true},
            axisLine: borderLineStyle,
            axisTick: borderLineStyle,
            splitLine: {show: false},
            data: use.xAxisData
          }],
          yAxis: [{
            splitNumber: 3,
            splitLine: {show: false},
            axisLine: {show: false}
          }],
          series: []
        };
        angular.forEach(use.series, function (series, i) {
          options.series.push(
            LookAndFeel.makeDataSeries(
              angular.merge({colors: LookAndFeel.colorSet(i)}, use.series[i])
            ));
        });
        if (options.series.length > 1) {
          options.legend = {show: true, data: []};
          angular.forEach(options.series, function (series) {
            options.legend.data.push(series.name);
          });
          options.grid.y = 30;
        }
        $scope.options = angular.merge(options, use.inject);
      }],
      template: '<div echart options="options" data="data"></div>'
    };
  }])

/**
 * Sparkline is an one data series line chart without axis labels.
 */
  .directive('barchart', [function () {
    return {
      restrict: 'E',
      scope: {options: '=', data: '='},
      controller: ['$scope', 'LookAndFeel', function ($scope, LookAndFeel) {
        var use = $scope.options || {};
        var colors = LookAndFeel.colorSet(0);
        var options = {
          tooltip: LookAndFeel.tooltip({color: colors.grid}),
          dataZoom: {show: false},
          grid: {borderWidth: 0, x: 5, y: 5, x2: 5, y2: 30},
          xAxis: [{
            axisLabel: {show: true},
            axisLine: {show: false},
            axisTick: {show: false},
            splitLine: {show: false},
            data: use.xAxisData
          }],
          yAxis: [{show: false}],
          series: [LookAndFeel.makeDataSeries(
            angular.merge({colors: colors}, use.series[0]))]
        };
        $scope.options = angular.merge(options, use.inject);
      }],
      template: '<div echart options="options" data="data"></div>'
    };
  }])

  .factory('LookAndFeel', function () {
    return {
      /** */
      tooltip: function (args) {
        var result = {
          trigger: 'axis',
          textStyle: {fontSize: 13},
          axisPointer: {type: 'none'},
          borderRadius: 2,
          showDelay: 0,
          transitionDuration: 0,
          formatter: args.formatter
        };
        if (args.color) {
          result.axisPointer = {
            type: 'line',
            lineStyle: {color: args.color, width: 2, type: 'dotted'}
          };
        }
        return result;
      },
      /** */
      makeDataSeries: function (args) {
        args.type = args.type || 'line';
        return angular.merge(args, {
          smooth: true,
          itemStyle: {
            normal: {
              areaStyle: {type: 'default', color: args.colors.area},
              lineStyle: {color: args.colors.line, width: 3}
            },
            emphasis: {color: args.colors.line}
          }
        });
      },
      /** */
      colorSet: function (i) {
        switch (i % 2) {
          case 1:
            return {
              line: 'rgb(110,119,215)',
              grid: 'rgba(110,119,215,.2)',
              area: 'rgb(129,242,250)'
            };
        }
        return {
          line: 'rgb(0,119,215)',
          grid: 'rgba(0,119,215,.2)',
          area: 'rgb(229,242,250)'
        };
      }
    };
  })
;