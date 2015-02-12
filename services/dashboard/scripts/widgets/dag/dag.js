/*
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

angular.module('app.widgets.dag', ['adf.provider'])
.value('appMasterUrl', location.href)
.config(function(dashboardProvider){
  dashboardProvider.widget('dag', {
    title: 'Dag',
    description: 'Dag widget',
    controller: 'dagCtrl',
    templateUrl: 'scripts/widgets/dag/dag.html',
    appId: -1,
    edit: {
      templateUrl: 'scripts/widgets/dag/edit.html',
      reload: false
    }
  });
})
.controller('dagCtrl', function($scope,$http){
  $scope.$on('appmaster-selected', function(event, appMasterSelected) {
    var url = location.origin+'/appmaster/'+appMasterSelected.appId+'?detail=true';
    $http.get(url).then(function(response){
      $scope.data = {}
      var json = response.data;//{"name":"dag","dag":{"vertices":["org.apache.gearpump.streaming.examples.complexdag.Node_2","org.apache.gearpump.streaming.examples.complexdag.Sink_3","org.apache.gearpump.streaming.examples.complexdag.Node_3","org.apache.gearpump.streaming.examples.complexdag.Node_4","org.apache.gearpump.streaming.examples.complexdag.Sink_0","org.apache.gearpump.streaming.examples.complexdag.Node_0","org.apache.gearpump.streaming.examples.complexdag.Sink_4","org.apache.gearpump.streaming.examples.complexdag.Sink_1","org.apache.gearpump.streaming.examples.complexdag.Source_0","org.apache.gearpump.streaming.examples.complexdag.Node_1","org.apache.gearpump.streaming.examples.complexdag.Sink_2","org.apache.gearpump.streaming.examples.complexdag.Source_1"],"edges":[["org.apache.gearpump.streaming.examples.complexdag.Node_2","org.apache.gearpump.partitioner.HashPartitioner","org.apache.gearpump.streaming.examples.complexdag.Node_3"],["org.apache.gearpump.streaming.examples.complexdag.Source_0","org.apache.gearpump.partitioner.HashPartitioner","org.apache.gearpump.streaming.examples.complexdag.Node_2"],["org.apache.gearpump.streaming.examples.complexdag.Node_0","org.apache.gearpump.partitioner.HashPartitioner","org.apache.gearpump.streaming.examples.complexdag.Sink_3"],["org.apache.gearpump.streaming.examples.complexdag.Source_0","org.apache.gearpump.partitioner.HashPartitioner","org.apache.gearpump.streaming.examples.complexdag.Node_1"],["org.apache.gearpump.streaming.examples.complexdag.Node_1","org.apache.gearpump.partitioner.HashPartitioner","org.apache.gearpump.streaming.examples.complexdag.Node_4"],["org.apache.gearpump.streaming.examples.complexdag.Node_3","org.apache.gearpump.partitioner.HashPartitioner","org.apache.gearpump.streaming.examples.complexdag.Sink_3"],["org.apache.gearpump.streaming.examples.complexdag.Node_1","org.apache.gearpump.partitioner.HashPartitioner","org.apache.gearpump.streaming.examples.complexdag.Node_3"],["org.apache.gearpump.streaming.examples.complexdag.Source_0","org.apache.gearpump.partitioner.HashPartitioner","org.apache.gearpump.streaming.examples.complexdag.Node_3"],["org.apache.gearpump.streaming.examples.complexdag.Node_1","org.apache.gearpump.partitioner.HashPartitioner","org.apache.gearpump.streaming.examples.complexdag.Sink_3"],["org.apache.gearpump.streaming.examples.complexdag.Source_0","org.apache.gearpump.partitioner.HashPartitioner","org.apache.gearpump.streaming.examples.complexdag.Sink_0"],["org.apache.gearpump.streaming.examples.complexdag.Source_1","org.apache.gearpump.partitioner.HashPartitioner","org.apache.gearpump.streaming.examples.complexdag.Node_0"],["org.apache.gearpump.streaming.examples.complexdag.Source_0","org.apache.gearpump.partitioner.HashPartitioner","org.apache.gearpump.streaming.examples.complexdag.Sink_1"],["org.apache.gearpump.streaming.examples.complexdag.Node_4","org.apache.gearpump.partitioner.HashPartitioner","org.apache.gearpump.streaming.examples.complexdag.Sink_3"],["org.apache.gearpump.streaming.examples.complexdag.Source_1","org.apache.gearpump.partitioner.HashPartitioner","org.apache.gearpump.streaming.examples.complexdag.Sink_4"],["org.apache.gearpump.streaming.examples.complexdag.Source_0","org.apache.gearpump.partitioner.HashPartitioner","org.apache.gearpump.streaming.examples.complexdag.Sink_2"]]}}
      $scope.data.nodes = [];
      var indexed = {};
      function lastPart(name) {
        var parts = name.split(/\./);
        return parts[parts.length-1];
      }
      json.dag.vertices.forEach(function(vertex,i) {
        var name = lastPart(vertex);
        $scope.data.nodes.push({name:name});
        indexed[name] = i;
      });
      $scope.data.links = [];
      json.dag.edges.forEach(function(edge,i) {
        var source = lastPart(edge[0]);
        var target = lastPart(edge[2]);
        var value = lastPart(edge[1]);
        $scope.data.links.push({source:indexed[source],target:indexed[target],value:value});
      });
      $scope.$broadcast('appmaster-data', {
        data: $scope.data
      });
    }, function(err){
      throw err; 
    });
  });
})
.directive('dag', function() {
  function dag(scope, el, attr){ 
    var color = d3.scale.category10(); 
    var width = 700; 
    var height = 700; 
    var trans=[0,0];
    var scale=1;
    var nodeRadius=10;
    var svg = d3.select(el[0]).append('svg').attr("width", width).attr("height", height).attr("pointer-events", "all");
    var fisheye = d3.fisheye().radius(100).power(3);
    var force = d3.layout.force()
      .gravity(.05)
      .linkDistance(200)
      .charge(-400)
      .size([width, height]);
    var link, node;

    function pathattr(d) {
      var dx = d.target.x - d.source.x,
          dy = d.target.y - d.source.y,
          path = "M0,0" + "L" + dx + "," + dy;
      return path;
    }

    function textattr(textselector) {
      textselector.attr("x", function(d) {
        return (d.target.x - d.source.x)/2;
      }).attr("y", function(d) {
        return (d.target.y - d.source.y)/2;
      }).attr("dy", ".25em").attr("dx","-.25em").text(function(d) { 
        return '+';//d.value; 
      });
    }

    function redraw() {
      trans=d3.event.translate;
      scale=d3.event.scale;
      svg.attr("transform", "translate(" + trans + ")" + " scale(" + scale + ")");
    }

    function tick() {
      node.attr("transform", function(d) { 
        return "translate(" + d.x + "," + d.y + ") scale(" + scale + ")"; 
      });
      link.attr("transform", function(d) { 
        return "translate(" + d.source.x + "," + d.source.y + ") scale(" + scale + ")"; 
      });
      link.select("path").attr("d", pathattr);
      textattr(link.select("text"));
    }

    svg.append("svg:defs").selectAll("marker")
      .data(["end"])   
      .enter().append("svg:marker")
      .attr("id", String)
      .attr("viewBox", "0 -5 10 10")
      .attr("refX", 10+nodeRadius)
      .attr("refY", 0.0)
      .attr("markerWidth", 6)
      .attr("markerHeight", 6)
      .attr("orient", "auto")
      .append("svg:path")
      .attr("d", "M0,-5L10,0L0,5");
    svg = svg.append('svg:g').call(d3.behavior.zoom().on("zoom", redraw)).append('svg:g');

    svg.append('svg:rect')
      .attr('width', width)
      .attr('height', height)
      .attr('fill', 'white');

    svg.on("mousemove", function() {
       fisheye.center(d3.mouse(this));
       node.each(function(d) { 
         d.display = fisheye(d); 
       });
       node.attr("transform", function(d) { 
         return "translate(" + d.display.x + "," + d.display.y + ") " + "scale(" + d.display.z + ")"; 
       });
       link.attr("transform", function(d) { 
         var source = d.source.display;
         return "translate(" + source.x + "," + source.y + ")";
       });
       function pathattr(d) {
         var dx = d.target.display.x - d.source.display.x,
             dy = d.target.display.y - d.source.display.y;
         var path = "M0,0" + "L" + dx + "," + dy;
         return path;
       }
       function textattr(textselector) {
         textselector.attr("x", function(d) {
           return (d.target.display.x - d.source.display.x)/2;
         }).attr("y", function(d) {
           return (d.target.display.y - d.source.display.y)/2;
         }).attr("dy", ".25em").attr("dx","-.25em");
       }
       link.select("path").attr("d", pathattr);
       textattr(link.select("text"));
    });

    scope.$on('appmaster-data', function(event, appMasterData) {
      if(node && link) return;
      force.nodes(appMasterData.data.nodes)
        .links(appMasterData.data.links)
        .on("tick", tick)
        .start();

      link = svg.selectAll(".link")
        .data(force.links())
        .enter().append("svg:g");

      link.append("svg:path").attr("marker-end", "url(#end)").attr("d", pathattr).attr("class","path");

      textattr(link.append("svg:text"))

      node = svg.selectAll(".node")
        .data(force.nodes())
        .enter().append("svg:g")
        .attr("class", "node")
        .call(force.drag);

      node.append("svg:circle").attr("r", nodeRadius);
      node.append("svg:text").attr("x", 12).attr("dy", ".25em").attr("dx", "-.25em").text(function(d){return d.name;});
    });
  } 
  return { link: dag, restrict: 'E', scope: { data: '=' } }; 
});
