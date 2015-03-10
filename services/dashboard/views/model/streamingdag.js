/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */
'use strict';

angular.module('dashboard.streamingdag', ['dashboard.metrics'])

  .factory('StreamingDag', ['Metrics', function (Metrics) {

    /** The constructor */
    function StreamingDag(id, processors, levels, edges) {
      this.id = id;
      this.processors = {};
      this.processorsLevels = {};
      this.edges = {};
      this.meter = {};
      this.histogram = {};

      processors.map(function (item) {
        this.processors[item[0]] = item[1];
      }, /* thisArg */ this);

      levels.map(function (item) {
        this.processorsLevels[item[0]] = item[1];
      }, /* thisArg */ this);

      edges.map(function (item) {
        var source = item[0];
        var target = item[2];
        var type = item[1];
        var id = source + '_' + target;
        this.edges[id] = {source: source, target: target, type: type};
      }, /* thisArg */ this);
    }

    StreamingDag.prototype = {

      /** update (or add) specified metrics in an array */
      updateMetricsArray: function(array) {
        array.map(function(item) {
          this.updateMetrics(item.value[0], item.value[1]);
        }, /* thisArg */ this);
      },

      /** update (or add) specified metrics */
      updateMetrics: function (name, data) {
        switch (name) {
          case 'org.apache.gearpump.metrics.Metrics.Meter':
            _update(Metrics.meter, this.meter, this.id);
            break;
          case 'org.apache.gearpump.metrics.Metrics.Histogram':
            _update(Metrics.histogram, this.histogram, this.id);
            break;
        }

        function _update(fn, coll, id) {
          var metric = fn(data);
          if (metric.name.appId === id) {
            var item = _getOrCreate(coll, metric.name.metric, {});
            var key = metric.name.processorId + '_' + metric.name.taskId;
            item[key] = metric.values;
            item[key].processorId = metric.name.processorId;
            item[key].taskId = metric.name.taskId;
          }
        }
      },

      /** Update node dataset on a vis widget. */
      updateVisGraphNodes: function (nodes, radiusRange) {
        var weights = {};
        angular.forEach(this.processors, function (_, id) {
          weights[id] = this._calculateProcessorWeight(id);
        }, this);
        weights[-1] = 0;
        var suggestRadius = _rangeMapper(weights, radiusRange);

        var diff = [];
        for (var id in this.processors) {
          if (this.processors.hasOwnProperty(id)) {
            var data = this.processors[id];
            var label = _lastPart(data.taskClass);
            var node = nodes.get(id);
            var newRadius = suggestRadius(weights[id]);
            if (!node || node.label !== label || node.radius !== newRadius) {
              diff.push({id: id, label: label, level: this.processorsLevels[id], radius: newRadius});
            }
          }
        }
        nodes.update(diff);
      },

      _calculateProcessorWeight: function (id) {
        var weight = 0;
        var sendThroughput = this.meter.sendThroughput;
        var receiveThroughput = this.meter.receiveThroughput;
        if (sendThroughput && receiveThroughput) {
          var tasks = this.processors[id].parallelism;
          weight += d3.sum(this._getMetricsByProcessor(id, tasks, sendThroughput, 'meanRate'));
          weight += d3.sum(this._getMetricsByProcessor(id, tasks, receiveThroughput, 'meanRate'));
        }
        return weight;
      },

      /** Return the difference of a Vis edge dataset. */
      updateVisGraphEdges: function (edges, widthRange, arrowSizeRange) {
        var bandwidths = {};
        angular.forEach(this.edges, function (_, id) {
          bandwidths[id] = this._calculateEdgeBandwidth(id);
        }, this);
        var suggestWidth = _rangeMapper(bandwidths, widthRange);
        var suggestArrowSize = _rangeMapper(bandwidths, arrowSizeRange);

        var diff = [];
        for (var id in this.edges) {
          if (this.edges.hasOwnProperty(id)) {
            var data = this.edges[id];
            var label = _lastPart(data.type);
            var edge = edges.get(id);
            var newWidth = suggestWidth(bandwidths[id]);
            var newArrowSize = suggestArrowSize(bandwidths[id]);
            if (!edge /*|| edge.label !== label*/ || edge.width !== newWidth) {
              diff.push({
                id: id,
                from: data.source,
                to: data.target,
                //label: label,
                width: newWidth,
                arrowScaleFactor: newArrowSize
              });
            }
          }
        }
        edges.update(diff);
      },

      _calculateEdgeBandwidth: function (id) {
        var bandwidth = 0;
        var sendThroughput = this.meter.sendThroughput;
        var receiveThroughput = this.meter.receiveThroughput;
        if (sendThroughput && receiveThroughput) {
          var parts = id.split('_');
          var sourceId = parseInt(parts[0]);
          var targetId = parseInt(parts[1]);
          var sourceTasks = this.processors[sourceId].parallelism;
          var targetTasks = this.processors[targetId].parallelism;
          var sourceSendThroughput = d3.sum(this._getMetricsByProcessor(sourceId, sourceTasks, sendThroughput, 'meanRate'));
          var targetReceiveThroughput = d3.sum(this._getMetricsByProcessor(targetId, targetTasks, receiveThroughput, 'meanRate'));
          bandwidth = Math.min(sourceSendThroughput, targetReceiveThroughput);
        }
        return bandwidth;
      },

      _getMetricsByProcessor: function (id, tasks, dictionary, metrics) {
        var values = [];
        for (var i = 0; i < tasks; i++) {
          var name = id + '_' + i;
          if (dictionary.hasOwnProperty(name)) {
            values.push(dictionary[name][metrics]);
          }
        }
        return values;
      }
    };

    function _getOrCreate(obj, prop, init) {
      if (!obj.hasOwnProperty(prop)) {
        obj[prop] = init;
      }
      return obj[prop];
    }

    function _lastPart(name) {
      var parts = name.split('.');
      return parts[parts.length - 1];
    }

    function _rangeMapper(dict, range) {
      var values = [];
      for (var key in dict) {
        if (dict.hasOwnProperty(key)) {
          var value = dict[key];
          values.push(value);
        }
      }
      return d3.scale.linear().domain([
        values.length > 0 ? d3.min(values) : 0,
        values.length > 0 ? d3.max(values) : 0])
        .range(range);
    }

    return StreamingDag;
  }])
;