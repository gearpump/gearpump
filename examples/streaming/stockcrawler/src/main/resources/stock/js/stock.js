/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
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

function initChart(chartid, tableid, stockId) {
  require.config({
    paths: {
      echarts: 'http://echarts.baidu.com/build/dist'
    }
  });

  require(
    [
      'echarts',
      'echarts/chart/line'
    ],

    function (ec) {
      // 基于准备好的dom，初始化echarts图表
      var myChart = ec.init(document.getElementById(chartid));
      var dataPoints = 100;
      var timeTicket;
      clearInterval(timeTicket);
      timeTicket = setInterval(function () {
        $.getJSON("report/" + stockId, function (json) {
          STOCK_NAME = json.name

          var maxDrawnDown = json.currentMax[0].max.price - json.currentMax[0].min.price;
          var time = new Date(json.currentMax[0].current.timestamp).toLocaleTimeString().replace(/^\D*/, '');
          // 动态数据接口 addData
          myChart.addData([
            [
              0,        // 系列索引
              maxDrawnDown.toFixed(2), // 新增数据
              false,     // 新增数据是否从队列头部插入
              false,     // 是否增加队列长度，false则自定删除原有数据，队头插入删队尾，队尾插入删队头
              time
            ],
            [
              1,        // 系列索引
              json.currentMax[0].current.price.toFixed(2), // 新增数据
              false,     // 新增数据是否从队列头部插入
              false,     // 是否增加队列长度，false则自定删除原有数据，队头插入删队尾，队尾插入删队头
              time
            ]
          ]);
          document.getElementById(chartid).style.display = "block"
          document.getElementById(tableid).innerHTML = "<pre>" + JSON.stringify(json, null, 2) + "</pre>"
        });
      }, 2000);

      var subtext_ = "Draw Down"

      var option = {
        title: {
          text: 'Stock Analysis',
          subtext: "Max " + subtext_
        },
        tooltip: {
          trigger: 'axis'
        },
        legend: {
          data: ["Current Price", "Current Draw Down"]
        },
        toolbox: {
          show: false,
          feature: {
            mark: {show: true},
            dataView: {show: true, readOnly: false},
            magicType: {show: true, type: ['line', 'bar']},
            restore: {show: true},
            saveAsImage: {show: true}
          }
        },
        dataZoom: {
          show: false,
          start: 0,
          end: 100
        },
        xAxis: [
          {
            type: 'category',
            boundaryGap: true,
            data: (function () {
              var now = new Date();
              var res = [];
              var len = dataPoints;
              while (len--) {
                res.unshift(now.toLocaleTimeString().replace(/^\D*/, ''));
                now = new Date(now - 2000);
              }
              return res;
            })()
          }
        ],
        yAxis: [
          {
            type: 'value',
            scale: true,
            name: subtext_ + ' 价格/元',
            boundaryGap: [0, 0.3]
          },
          {
            type: 'value',
            scale: true,
            name: 'Current 价格/元',
            boundaryGap: [0, 0.1]
          }
        ],
        series: [
          {
            name: "Current Draw Down",
            type: 'line',
            data: (function () {
              var res = [];
              var len = dataPoints;
              while (len--) {
                res.push(0);
              }
              return res;
            })()
          },
          {
            name: "Current Price",
            type: 'line',
            yAxisIndex: 1,
            data: (function () {
              var res = [];
              var len = dataPoints;
              while (len--) {
                res.push(0);
              }
              return res;
            })()
          }
        ]
      };

      // 为echarts对象加载数据
      myChart.setOption(option);
    }
  );
}