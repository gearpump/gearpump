var myChart = echarts.init(document.getElementById("mychart"))

echarts.util.mapData.params.params.football = {
    getGeoJson: function (callback) {
        $.ajax({
            url: "../svg/city.svg",
            dataType: 'xml',
            success: function(xml) {
                callback(xml)
            }
        });
    }
}

function updateRecords(tableId) {
   $.getJSON( "records", function( json ) {
     var tableStr = "<table class=\"dataintable\" style=\"margin-left: 5px;\">";
     tableStr += "<tr><th>Over Speed Vehicle ID</th><th>Speed</th><th>Location</th><th>Time</th></tr>";
     var records = json.records;
     for(var i = 0; i < Math.min(records.length, 20); i++) {
       var record = records[i];
       var vehicleId = record.vehicleId;
       var location = record.locationId.split("_");
       var speed = record.speed;
       var row = location[1];
       var column = location[2];
       var time = new Date(Number(record.timestamp)).toLocaleTimeString().replace(/^\D*/,'');
       tableStr += "<tr><td>" + vehicleId + "</td>";
       tableStr += "<td>" + speed + "km/h </td>"
       tableStr += "<td>(" + row + ", "+ column + ")</td>";
       tableStr += "<td>" + time + "</td></tr>";
     }
     if(records.length < 20) {
       for(var i = records.length; i < 20; i++) {
          tableStr += "<tr><td></td>";
          tableStr += "<td> </td>"
          tableStr += "<td> </td>";
          tableStr += "<td> </td></tr>";
       }
     }
     tableStr += "</table>"
     document.getElementById(tableId).innerHTML = tableStr;
   }
   )
}

function initChart(chartid, vehicleId) {
      // 基于准备好的dom，初始化echarts图表
      $.getJSON( "trace/" + vehicleId, function( json ) {
        // 为echarts对象加载数据
        var records = json.records;
        var timeLine = new Array(records.length);
        var markPoints = new Array(records.length);
        var markLines = new Array(records.length - 1);
        var options_ = new Array(records.length - 2);
        var lastPoint = null;
        for(var i = 0; i < records.length; i++) {
          var record = records[i];
          var vehicleId = record.vehicleId;
          var location = record.locationId.split("_");
          var row = location[1];
          var column = location[2];
          var time = new Date(Number(record.timeStamp)).toLocaleTimeString().replace(/^\D*/,'');
          timeLine[i] = time;
          var currentPonit = {name: row, value: column, geoCoord:[row * 20, column * 10]};
          markPoints[i] = currentPonit;
          if(i >= 1) {
            markLines[i - 1] = [lastPoint, currentPonit];
          }
          lastPoint = currentPonit;
        }
        options_[0] =
          {
            title : {
              text: 'Vehicle trace'
            },
            tooltip : {
              trigger: 'item'
            },
            toolbox: {
              show : false,
              feature : {
                mark : {show: true},
                dataView : {show: true, readOnly: false},
                magicType : {show: true, type: ['line', 'bar']},
                restore : {show: true},
                saveAsImage : {show: true}
              }
            },
            series : [
              {
                name: 'Vehicle trace',
                type: 'map',
                mapType: 'football',
                mapLocation:{
                  y: 30,
                  height: 430
                },
                itemStyle:{
                  normal:{label:{show:false}},
                  emphasis:{label:{show:false}}
                },
                data:[
                  {name: 'City', hoverable: false, itemStyle:{normal:{label:{show:false}}}}
                ],
                markPoint : {
                  symbol:'circle',
                  symbolSize : 8,
                  itemStyle : {
                    normal: {
                      borderWidth:1,
                      color: 'blue',
                      lineStyle: {
                        type: 'solid'
                      }
                    }
                  },
                  data: markPoints
                },
                markLine : {
                  smooth:true,
                  effect : {
                    show: true,
                    scaleSize: 1.5,
                    period: 1.5,
                    color: '#fff'
                  },
                  itemStyle : {
                    normal: {
                      borderWidth:1,
                      color: 'black',
                      lineStyle: {
                        type: 'solid'
                      }
                    }
                  },
                  data: [markLines[0]]
                }
              }
            ]
          }
        for(var i = 1; i < markLines.length; i++){
          options_[i] =
          {
            series: [
            {
              markPoint : {
                data: markPoints
              },
              markLine : {
                data: [markLines[i]]
              }
            }
            ]
          }
        }
        var option = {
          timeline : {
            type: 'number',
            playInterval:500,
            autoPlay:true,
            data: timeLine
          },
          options: options_
        };
        myChart.setOption(option);
      });
}