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

angular.module('app.widgets.dag', ['adf.provider', 'nvd3'])
  .config(function(dashboardProvider){
    dashboardProvider
      .widget('dag', {
        title: 'Dag',
        description: 'Dag widget',
        controller: 'dagCtrl',
        templateUrl: 'scripts/widgets/dag/dag.html',
        edit: {
          templateUrl: 'scripts/widgets/dag/edit.html',
          reload: false
        }
      });
  })
  .controller('dagCtrl', function($scope){
  $scope.data = {}
  //sample JSON
  var json = {"name":"dag","dag":{"vertices":["org.apache.gearpump.streaming.examples.complexdag.Node_2","org.apache.gearpump.streaming.examples.complexdag.Sink_3","org.apache.gearpump.streaming.examples.complexdag.Node_3","org.apache.gearpump.streaming.examples.complexdag.Node_4","org.apache.gearpump.streaming.examples.complexdag.Sink_0","org.apache.gearpump.streaming.examples.complexdag.Node_0","org.apache.gearpump.streaming.examples.complexdag.Sink_4","org.apache.gearpump.streaming.examples.complexdag.Sink_1","org.apache.gearpump.streaming.examples.complexdag.Source_0","org.apache.gearpump.streaming.examples.complexdag.Node_1","org.apache.gearpump.streaming.examples.complexdag.Sink_2","org.apache.gearpump.streaming.examples.complexdag.Source_1"],"edges":[["org.apache.gearpump.streaming.examples.complexdag.Node_2","org.apache.gearpump.partitioner.HashPartitioner","org.apache.gearpump.streaming.examples.complexdag.Node_3"],["org.apache.gearpump.streaming.examples.complexdag.Source_0","org.apache.gearpump.partitioner.HashPartitioner","org.apache.gearpump.streaming.examples.complexdag.Node_2"],["org.apache.gearpump.streaming.examples.complexdag.Node_0","org.apache.gearpump.partitioner.HashPartitioner","org.apache.gearpump.streaming.examples.complexdag.Sink_3"],["org.apache.gearpump.streaming.examples.complexdag.Source_0","org.apache.gearpump.partitioner.HashPartitioner","org.apache.gearpump.streaming.examples.complexdag.Node_1"],["org.apache.gearpump.streaming.examples.complexdag.Node_1","org.apache.gearpump.partitioner.HashPartitioner","org.apache.gearpump.streaming.examples.complexdag.Node_4"],["org.apache.gearpump.streaming.examples.complexdag.Node_3","org.apache.gearpump.partitioner.HashPartitioner","org.apache.gearpump.streaming.examples.complexdag.Sink_3"],["org.apache.gearpump.streaming.examples.complexdag.Node_1","org.apache.gearpump.partitioner.HashPartitioner","org.apache.gearpump.streaming.examples.complexdag.Node_3"],["org.apache.gearpump.streaming.examples.complexdag.Source_0","org.apache.gearpump.partitioner.HashPartitioner","org.apache.gearpump.streaming.examples.complexdag.Node_3"],["org.apache.gearpump.streaming.examples.complexdag.Node_1","org.apache.gearpump.partitioner.HashPartitioner","org.apache.gearpump.streaming.examples.complexdag.Sink_3"],["org.apache.gearpump.streaming.examples.complexdag.Source_0","org.apache.gearpump.partitioner.HashPartitioner","org.apache.gearpump.streaming.examples.complexdag.Sink_0"],["org.apache.gearpump.streaming.examples.complexdag.Source_1","org.apache.gearpump.partitioner.HashPartitioner","org.apache.gearpump.streaming.examples.complexdag.Node_0"],["org.apache.gearpump.streaming.examples.complexdag.Source_0","org.apache.gearpump.partitioner.HashPartitioner","org.apache.gearpump.streaming.examples.complexdag.Sink_1"],["org.apache.gearpump.streaming.examples.complexdag.Node_4","org.apache.gearpump.partitioner.HashPartitioner","org.apache.gearpump.streaming.examples.complexdag.Sink_3"],["org.apache.gearpump.streaming.examples.complexdag.Source_1","org.apache.gearpump.partitioner.HashPartitioner","org.apache.gearpump.streaming.examples.complexdag.Sink_4"],["org.apache.gearpump.streaming.examples.complexdag.Source_0","org.apache.gearpump.partitioner.HashPartitioner","org.apache.gearpump.streaming.examples.complexdag.Sink_2"]]}}
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
}).directive('dag', function() { 
  function link(scope, el, attr){ 
    var color = d3.scale.category10(); 
    var width = 700; 
    var height = 700; 
    var svg = d3.select(el[0]).append('svg').attr("width", width).attr("height", height);
    var force = d3.layout.force()
      .gravity(.05)
      .linkDistance(60)
      .charge(-300)
      .size([width, height])
      .nodes(scope.data.nodes)
      .links(scope.data.links)
      .on("tick", tick)
      .start();

    svg.append("svg:defs").selectAll("marker")
      .data(["end"])   
      .enter().append("svg:marker")
      .attr("id", String)
      .attr("viewBox", "0 -5 10 10")
      .attr("refX", 15)
      .attr("refY", -1.5)
      .attr("markerWidth", 6)
      .attr("markerHeight", 6)
      .attr("orient", "auto")
      .append("svg:path")
      .attr("d", "M0,-5L10,0L0,5");

    var path = svg.append("svg:g").selectAll("path")
      .data(force.links())
      .enter().append("svg:path")
      .attr("class", "link")
      .attr("marker-end", "url(#end)");

    var node = svg.selectAll(".node")
      .data(force.nodes())
      .enter().append("g")
      .attr("class", "node")
      .call(force.drag);

    node.append("circle").attr("r", 5);
    node.append("text").attr("x", 12).attr("dy", ".35em").text(function(d) { return d.name; });

    function tick() {
        path.attr("d", function(d) {
            var dx = d.target.x - d.source.x,
                dy = d.target.y - d.source.y,
                dr = Math.sqrt(dx * dx + dy * dy);
            return "M" + 
                d.source.x + "," + 
                d.source.y + "A" + 
                dr + "," + dr + " 0 0,1 " + 
                d.target.x + "," + 
                d.target.y;
        });
    
        node.attr("transform", function(d) { return "translate(" + d.x + "," + d.y + ")"; });
    }
  } 
  return { link: link, restrict: 'E', scope: { data: '=' } }; 
});
