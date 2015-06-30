/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gearpump.dashboard.controllers

import org.apache.gearpump.shared.Messages.{Histogram, Meter, HistoryMetrics, StreamingAppMasterDataDetail}
import utest._

import scala.scalajs.js.{JSON, undefined}

object AppDagCtrlSpec extends TestSuite {
  implicit val ec = utest.ExecutionContext.RunNow

val metricsJSON =
  """
    |{
    |"appId": 1,
    |"path": "path",
    |"metrics":
    |[
    |  {
    |    "time": "1435595418893",
    |    "value": {
    |      "typeName": "org.apache.gearpump.shared.Messages.Histogram",
    |      "json": "[\"org.apache.gearpump.shared.Messages.Histogram\",{\"name\":\"app1.processor8.task0.receiveLatency\",\"count\":\"0\",\"min\":\"0\",\"max\":\"0\",\"mean\":0,\"stddev\":0,\"median\":0,\"p75\":0,\"p95\":0,\"p98\":0,\"p99\":0,\"p999\":0}]"
    |    }
    |  },
    |  {
    |    "time": "1435595418898",
    |    "value": {
    |      "typeName": "org.apache.gearpump.shared.Messages.Meter",
    |      "json": "[\"org.apache.gearpump.shared.Messages.Meter\",{\"name\":\"app1.processor4.task0.receiveThroughput\",\"count\":\"28\",\"meanRate\":5.1706479477387415,\"m1\":5.6,\"m5\":5.6,\"m15\":5.6,\"rateUnit\":\"events/second\"}]"
    |    }
    |  },
    |  {
    |    "time": "1435595418860",
    |    "value": {
    |      "typeName": "org.apache.gearpump.shared.Messages.Meter",
    |      "json": "[\"org.apache.gearpump.shared.Messages.Meter\",{\"name\":\"app1.processor0.task0.sendThroughput\",\"count\":\"60\",\"meanRate\":11.125331910046382,\"m1\":12,\"m5\":12,\"m15\":12,\"rateUnit\":\"events/second\"}]"
    |    }
    |  },
    |  {
    |    "time": "1435595418893",
    |    "value": {
    |      "typeName": "org.apache.gearpump.shared.Messages.Histogram",
    |      "json": "[\"org.apache.gearpump.shared.Messages.Histogram\",{\"name\":\"app1.processor7.task0.processTime\",\"count\":\"6\",\"min\":\"0\",\"max\":\"0\",\"mean\":0,\"stddev\":0,\"median\":0,\"p75\":0,\"p95\":0,\"p98\":0,\"p99\":0,\"p999\":0}]"
    |    }
    |  },
    |  {
    |    "time": "1435595418861",
    |    "value": {
    |      "typeName": "org.apache.gearpump.shared.Messages.Meter",
    |      "json": "[\"org.apache.gearpump.shared.Messages.Meter\",{\"name\":\"app1.processor10.task0.sendThroughput\",\"count\":\"0\",\"meanRate\":0,\"m1\":0,\"m5\":0,\"m15\":0,\"rateUnit\":\"events/second\"}]"
    |    }
    |  },
    |  {
    |    "time": "1435595418894",
    |    "value": {
    |      "typeName": "org.apache.gearpump.shared.Messages.Meter",
    |      "json": "[\"org.apache.gearpump.shared.Messages.Meter\",{\"name\":\"app1.processor11.task0.receiveThroughput\",\"count\":\"8\",\"meanRate\":1.474732767878121,\"m1\":1.6,\"m5\":1.6,\"m15\":1.6,\"rateUnit\":\"events/second\"}]"
    |    }
    |  },
    |  {
    |    "time": "1435595418899",
    |    "value": {
    |      "typeName": "org.apache.gearpump.shared.Messages.Meter",
    |      "json": "[\"org.apache.gearpump.shared.Messages.Meter\",{\"name\":\"app1.processor7.task0.sendThroughput\",\"count\":\"0\",\"meanRate\":0,\"m1\":0,\"m5\":0,\"m15\":0,\"rateUnit\":\"events/second\"}]"
    |    }
    |  },
    |  {
    |    "time": "1435595418892",
    |    "value": {
    |      "typeName": "org.apache.gearpump.shared.Messages.Histogram",
    |      "json": "[\"org.apache.gearpump.shared.Messages.Histogram\",{\"name\":\"app1.processor4.task0.receiveLatency\",\"count\":\"1\",\"min\":\"1\",\"max\":\"1\",\"mean\":1,\"stddev\":0,\"median\":1,\"p75\":1,\"p95\":1,\"p98\":1,\"p99\":1,\"p999\":1}]"
    |    }
    |  },
    |  {
    |    "time": "1435595418862",
    |    "value": {
    |      "typeName": "org.apache.gearpump.shared.Messages.Meter",
    |      "json": "[\"org.apache.gearpump.shared.Messages.Meter\",{\"name\":\"app1.processor5.task0.sendThroughput\",\"count\":\"30\",\"meanRate\":5.563408987313358,\"m1\":6,\"m5\":6,\"m15\":6,\"rateUnit\":\"events/second\"}]"
    |    }
    |  },
    |  {
    |    "time": "1435595418893",
    |    "value": {
    |      "typeName": "org.apache.gearpump.shared.Messages.Histogram",
    |      "json": "[\"org.apache.gearpump.shared.Messages.Histogram\",{\"name\":\"app1.processor7.task0.receiveLatency\",\"count\":\"2\",\"min\":\"0\",\"max\":\"4\",\"mean\":2,\"stddev\":2.8284271247461903,\"median\":2,\"p75\":4,\"p95\":4,\"p98\":4,\"p99\":4,\"p999\":4}]"
    |    }
    |  },
    |  {
    |    "time": "1435595418859",
    |    "value": {
    |      "typeName": "org.apache.gearpump.shared.Messages.Histogram",
    |      "json": "[\"org.apache.gearpump.shared.Messages.Histogram\",{\"name\":\"app1.processor9.task0.processTime\",\"count\":\"1\",\"min\":\"0\",\"max\":\"0\",\"mean\":0,\"stddev\":0,\"median\":0,\"p75\":0,\"p95\":0,\"p98\":0,\"p99\":0,\"p999\":0}]"
    |    }
    |  },
    |  {
    |    "time": "1435595418855",
    |    "value": {
    |      "typeName": "org.apache.gearpump.shared.Messages.Histogram",
    |      "json": "[\"org.apache.gearpump.shared.Messages.Histogram\",{\"name\":\"app1.processor0.task0.receiveLatency\",\"count\":\"0\",\"min\":\"0\",\"max\":\"0\",\"mean\":0,\"stddev\":0,\"median\":0,\"p75\":0,\"p95\":0,\"p98\":0,\"p99\":0,\"p999\":0}]"
    |    }
    |  },
    |  {
    |    "time": "1435595418892",
    |    "value": {
    |      "typeName": "org.apache.gearpump.shared.Messages.Histogram",
    |      "json": "[\"org.apache.gearpump.shared.Messages.Histogram\",{\"name\":\"app1.processor3.task0.processTime\",\"count\":\"1\",\"min\":\"0\",\"max\":\"0\",\"mean\":0,\"stddev\":0,\"median\":0,\"p75\":0,\"p95\":0,\"p98\":0,\"p99\":0,\"p999\":0}]"
    |    }
    |  },
    |  {
    |    "time": "1435595418855",
    |    "value": {
    |      "typeName": "org.apache.gearpump.shared.Messages.Histogram",
    |      "json": "[\"org.apache.gearpump.shared.Messages.Histogram\",{\"name\":\"app1.processor0.task0.processTime\",\"count\":\"1\",\"min\":\"1\",\"max\":\"1\",\"mean\":1,\"stddev\":0,\"median\":1,\"p75\":1,\"p95\":1,\"p98\":1,\"p99\":1,\"p999\":1}]"
    |    }
    |  },
    |  {
    |    "time": "1435595418899",
    |    "value": {
    |      "typeName": "org.apache.gearpump.shared.Messages.Meter",
    |      "json": "[\"org.apache.gearpump.shared.Messages.Meter\",{\"name\":\"app1.processor8.task0.receiveThroughput\",\"count\":\"8\",\"meanRate\":1.474554786585802,\"m1\":1.6,\"m5\":1.6,\"m15\":1.6,\"rateUnit\":\"events/second\"}]"
    |    }
    |  },
    |  {
    |    "time": "1435595418899",
    |    "value": {
    |      "typeName": "org.apache.gearpump.shared.Messages.Meter",
    |      "json": "[\"org.apache.gearpump.shared.Messages.Meter\",{\"name\":\"app1.processor8.task0.sendThroughput\",\"count\":\"10\",\"meanRate\":1.8431532311445549,\"m1\":2,\"m5\":2,\"m15\":2,\"rateUnit\":\"events/second\"}]"
    |    }
    |  },
    |  {
    |    "time": "1435595418897",
    |    "value": {
    |      "typeName": "org.apache.gearpump.shared.Messages.Meter",
    |      "json": "[\"org.apache.gearpump.shared.Messages.Meter\",{\"name\":\"app1.processor3.task0.receiveThroughput\",\"count\":\"8\",\"meanRate\":1.4746225192382731,\"m1\":1.6,\"m5\":1.6,\"m15\":1.6,\"rateUnit\":\"events/second\"}]"
    |    }
    |  },
    |  {
    |    "time": "1435595418856",
    |    "value": {
    |      "typeName": "org.apache.gearpump.shared.Messages.Histogram",
    |      "json": "[\"org.apache.gearpump.shared.Messages.Histogram\",{\"name\":\"app1.processor1.task0.processTime\",\"count\":\"1\",\"min\":\"0\",\"max\":\"0\",\"mean\":0,\"stddev\":0,\"median\":0,\"p75\":0,\"p95\":0,\"p98\":0,\"p99\":0,\"p999\":0}]"
    |    }
    |  },
    |  {
    |    "time": "1435595418862",
    |    "value": {
    |      "typeName": "org.apache.gearpump.shared.Messages.Meter",
    |      "json": "[\"org.apache.gearpump.shared.Messages.Meter\",{\"name\":\"app1.processor9.task0.receiveThroughput\",\"count\":\"7\",\"meanRate\":1.297930126894312,\"m1\":1.4,\"m5\":1.4,\"m15\":1.4,\"rateUnit\":\"events/second\"}]"
    |    }
    |  },
    |  {
    |    "time": "1435595418863",
    |    "value": {
    |      "typeName": "org.apache.gearpump.shared.Messages.Meter",
    |      "json": "[\"org.apache.gearpump.shared.Messages.Meter\",{\"name\":\"app1.processor9.task0.sendThroughput\",\"count\":\"20\",\"meanRate\":3.708313630614926,\"m1\":4,\"m5\":4,\"m15\":4,\"rateUnit\":\"events/second\"}]"
    |    }
    |  },
    |  {
    |    "time": "1435595418860",
    |    "value": {
    |      "typeName": "org.apache.gearpump.shared.Messages.Meter",
    |      "json": "[\"org.apache.gearpump.shared.Messages.Meter\",{\"name\":\"app1.processor1.task0.receiveThroughput\",\"count\":\"9\",\"meanRate\":1.6691095122936113,\"m1\":1.8,\"m5\":1.8,\"m15\":1.8,\"rateUnit\":\"events/second\"}]"
    |    }
    |  },
    |  {
    |    "time": "1435595418857",
    |    "value": {
    |      "typeName": "org.apache.gearpump.shared.Messages.Histogram",
    |      "json": "[\"org.apache.gearpump.shared.Messages.Histogram\",{\"name\":\"app1.processor5.task0.processTime\",\"count\":\"1\",\"min\":\"1\",\"max\":\"1\",\"mean\":1,\"stddev\":0,\"median\":1,\"p75\":1,\"p95\":1,\"p98\":1,\"p99\":1,\"p999\":1}]"
    |    }
    |  },
    |  {
    |    "time": "1435595418859",
    |    "value": {
    |      "typeName": "org.apache.gearpump.shared.Messages.Meter",
    |      "json": "[\"org.apache.gearpump.shared.Messages.Meter\",{\"name\":\"app1.processor0.task0.receiveThroughput\",\"count\":\"3\",\"meanRate\":0.556279575261883,\"m1\":0.6,\"m5\":0.6,\"m15\":0.6,\"rateUnit\":\"events/second\"}]"
    |    }
    |  },
    |  {
    |    "time": "1435595418897",
    |    "value": {
    |      "typeName": "org.apache.gearpump.shared.Messages.Meter",
    |      "json": "[\"org.apache.gearpump.shared.Messages.Meter\",{\"name\":\"app1.processor2.task0.sendThroughput\",\"count\":\"0\",\"meanRate\":0,\"m1\":0,\"m5\":0,\"m15\":0,\"rateUnit\":\"events/second\"}]"
    |    }
    |  },
    |  {
    |    "time": "1435595418899",
    |    "value": {
    |      "typeName": "org.apache.gearpump.shared.Messages.Meter",
    |      "json": "[\"org.apache.gearpump.shared.Messages.Meter\",{\"name\":\"app1.processor7.task0.receiveThroughput\",\"count\":\"59\",\"meanRate\":10.874958479546766,\"m1\":11.799999999999999,\"m5\":11.799999999999999,\"m15\":11.799999999999999,\"rateUnit\":\"events/second\"}]"
    |    }
    |  },
    |  {
    |    "time": "1435595418896",
    |    "value": {
    |      "typeName": "org.apache.gearpump.shared.Messages.Meter",
    |      "json": "[\"org.apache.gearpump.shared.Messages.Meter\",{\"name\":\"app1.processor11.task0.sendThroughput\",\"count\":\"10\",\"meanRate\":1.8433727760777074,\"m1\":2,\"m5\":2,\"m15\":2,\"rateUnit\":\"events/second\"}]"
    |    }
    |  },
    |  {
    |    "time": "1435595418858",
    |    "value": {
    |      "typeName": "org.apache.gearpump.shared.Messages.Histogram",
    |      "json": "[\"org.apache.gearpump.shared.Messages.Histogram\",{\"name\":\"app1.processor5.task0.receiveLatency\",\"count\":\"0\",\"min\":\"0\",\"max\":\"0\",\"mean\":0,\"stddev\":0,\"median\":0,\"p75\":0,\"p95\":0,\"p98\":0,\"p99\":0,\"p999\":0}]"
    |    }
    |  },
    |  {
    |    "time": "1435595418890",
    |    "value": {
    |      "typeName": "org.apache.gearpump.shared.Messages.Histogram",
    |      "json": "[\"org.apache.gearpump.shared.Messages.Histogram\",{\"name\":\"app1.processor11.task0.processTime\",\"count\":\"1\",\"min\":\"0\",\"max\":\"0\",\"mean\":0,\"stddev\":0,\"median\":0,\"p75\":0,\"p95\":0,\"p98\":0,\"p99\":0,\"p999\":0}]"
    |    }
    |  },
    |  {
    |    "time": "1435595418893",
    |    "value": {
    |      "typeName": "org.apache.gearpump.shared.Messages.Histogram",
    |      "json": "[\"org.apache.gearpump.shared.Messages.Histogram\",{\"name\":\"app1.processor8.task0.processTime\",\"count\":\"1\",\"min\":\"0\",\"max\":\"0\",\"mean\":0,\"stddev\":0,\"median\":0,\"p75\":0,\"p95\":0,\"p98\":0,\"p99\":0,\"p999\":0}]"
    |    }
    |  },
    |  {
    |    "time": "1435595418858",
    |    "value": {
    |      "typeName": "org.apache.gearpump.shared.Messages.Histogram",
    |      "json": "[\"org.apache.gearpump.shared.Messages.Histogram\",{\"name\":\"app1.processor6.task0.receiveLatency\",\"count\":\"0\",\"min\":\"0\",\"max\":\"0\",\"mean\":0,\"stddev\":0,\"median\":0,\"p75\":0,\"p95\":0,\"p98\":0,\"p99\":0,\"p999\":0}]"
    |    }
    |  },
    |  {
    |    "time": "1435595418857",
    |    "value": {
    |      "typeName": "org.apache.gearpump.shared.Messages.Histogram",
    |      "json": "[\"org.apache.gearpump.shared.Messages.Histogram\",{\"name\":\"app1.processor10.task0.processTime\",\"count\":\"1\",\"min\":\"0\",\"max\":\"0\",\"mean\":0,\"stddev\":0,\"median\":0,\"p75\":0,\"p95\":0,\"p98\":0,\"p99\":0,\"p999\":0}]"
    |    }
    |  },
    |  {
    |    "time": "1435595418898",
    |    "value": {
    |      "typeName": "org.apache.gearpump.shared.Messages.Meter",
    |      "json": "[\"org.apache.gearpump.shared.Messages.Meter\",{\"name\":\"app1.processor4.task0.sendThroughput\",\"count\":\"30\",\"meanRate\":5.532103395685169,\"m1\":6,\"m5\":6,\"m15\":6,\"rateUnit\":\"events/second\"}]"
    |    }
    |  },
    |  {
    |    "time": "1435595418862",
    |    "value": {
    |      "typeName": "org.apache.gearpump.shared.Messages.Meter",
    |      "json": "[\"org.apache.gearpump.shared.Messages.Meter\",{\"name\":\"app1.processor6.task0.sendThroughput\",\"count\":\"0\",\"meanRate\":0,\"m1\":0,\"m5\":0,\"m15\":0,\"rateUnit\":\"events/second\"}]"
    |    }
    |  },
    |  {
    |    "time": "1435595418897",
    |    "value": {
    |      "typeName": "org.apache.gearpump.shared.Messages.Meter",
    |      "json": "[\"org.apache.gearpump.shared.Messages.Meter\",{\"name\":\"app1.processor2.task0.receiveThroughput\",\"count\":\"9\",\"meanRate\":1.6589784967205394,\"m1\":1.8,\"m5\":1.8,\"m15\":1.8,\"rateUnit\":\"events/second\"}]"
    |    }
    |  },
    |  {
    |    "time": "1435595418860",
    |    "value": {
    |      "typeName": "org.apache.gearpump.shared.Messages.Meter",
    |      "json": "[\"org.apache.gearpump.shared.Messages.Meter\",{\"name\":\"app1.processor1.task0.sendThroughput\",\"count\":\"0\",\"meanRate\":0,\"m1\":0,\"m5\":0,\"m15\":0,\"rateUnit\":\"events/second\"}]"
    |    }
    |  },
    |  {
    |    "time": "1435595418858",
    |    "value": {
    |      "typeName": "org.apache.gearpump.shared.Messages.Histogram",
    |      "json": "[\"org.apache.gearpump.shared.Messages.Histogram\",{\"name\":\"app1.processor6.task0.processTime\",\"count\":\"1\",\"min\":\"0\",\"max\":\"0\",\"mean\":0,\"stddev\":0,\"median\":0,\"p75\":0,\"p95\":0,\"p98\":0,\"p99\":0,\"p999\":0}]"
    |    }
    |  },
    |  {
    |    "time": "1435595418891",
    |    "value": {
    |      "typeName": "org.apache.gearpump.shared.Messages.Histogram",
    |      "json": "[\"org.apache.gearpump.shared.Messages.Histogram\",{\"name\":\"app1.processor11.task0.receiveLatency\",\"count\":\"0\",\"min\":\"0\",\"max\":\"0\",\"mean\":0,\"stddev\":0,\"median\":0,\"p75\":0,\"p95\":0,\"p98\":0,\"p99\":0,\"p999\":0}]"
    |    }
    |  },
    |  {
    |    "time": "1435595418891",
    |    "value": {
    |      "typeName": "org.apache.gearpump.shared.Messages.Histogram",
    |      "json": "[\"org.apache.gearpump.shared.Messages.Histogram\",{\"name\":\"app1.processor2.task0.receiveLatency\",\"count\":\"0\",\"min\":\"0\",\"max\":\"0\",\"mean\":0,\"stddev\":0,\"median\":0,\"p75\":0,\"p95\":0,\"p98\":0,\"p99\":0,\"p999\":0}]"
    |    }
    |  },
    |  {
    |    "time": "1435595418856",
    |    "value": {
    |      "typeName": "org.apache.gearpump.shared.Messages.Histogram",
    |      "json": "[\"org.apache.gearpump.shared.Messages.Histogram\",{\"name\":\"app1.processor1.task0.receiveLatency\",\"count\":\"0\",\"min\":\"0\",\"max\":\"0\",\"mean\":0,\"stddev\":0,\"median\":0,\"p75\":0,\"p95\":0,\"p98\":0,\"p99\":0,\"p999\":0}]"
    |    }
    |  },
    |  {
    |    "time": "1435595418861",
    |    "value": {
    |      "typeName": "org.apache.gearpump.shared.Messages.Meter",
    |      "json": "[\"org.apache.gearpump.shared.Messages.Meter\",{\"name\":\"app1.processor5.task0.receiveThroughput\",\"count\":\"6\",\"meanRate\":1.1126918555593872,\"m1\":1.2,\"m5\":1.2,\"m15\":1.2,\"rateUnit\":\"events/second\"}]"
    |    }
    |  },
    |  {
    |    "time": "1435595418892",
    |    "value": {
    |      "typeName": "org.apache.gearpump.shared.Messages.Histogram",
    |      "json": "[\"org.apache.gearpump.shared.Messages.Histogram\",{\"name\":\"app1.processor3.task0.receiveLatency\",\"count\":\"0\",\"min\":\"0\",\"max\":\"0\",\"mean\":0,\"stddev\":0,\"median\":0,\"p75\":0,\"p95\":0,\"p98\":0,\"p99\":0,\"p999\":0}]"
    |    }
    |  },
    |  {
    |    "time": "1435595418891",
    |    "value": {
    |      "typeName": "org.apache.gearpump.shared.Messages.Histogram",
    |      "json": "[\"org.apache.gearpump.shared.Messages.Histogram\",{\"name\":\"app1.processor2.task0.processTime\",\"count\":\"1\",\"min\":\"0\",\"max\":\"0\",\"mean\":0,\"stddev\":0,\"median\":0,\"p75\":0,\"p95\":0,\"p98\":0,\"p99\":0,\"p999\":0}]"
    |    }
    |  },
    |  {
    |    "time": "1435595418862",
    |    "value": {
    |      "typeName": "org.apache.gearpump.shared.Messages.Meter",
    |      "json": "[\"org.apache.gearpump.shared.Messages.Meter\",{\"name\":\"app1.processor6.task0.receiveThroughput\",\"count\":\"9\",\"meanRate\":1.6690219217873472,\"m1\":1.8,\"m5\":1.8,\"m15\":1.8,\"rateUnit\":\"events/second\"}]"
    |    }
    |  },
    |  {
    |    "time": "1435595418861",
    |    "value": {
    |      "typeName": "org.apache.gearpump.shared.Messages.Meter",
    |      "json": "[\"org.apache.gearpump.shared.Messages.Meter\",{\"name\":\"app1.processor10.task0.receiveThroughput\",\"count\":\"9\",\"meanRate\":1.669108475620026,\"m1\":1.8,\"m5\":1.8,\"m15\":1.8,\"rateUnit\":\"events/second\"}]"
    |    }
    |  },
    |  {
    |    "time": "1435595418859",
    |    "value": {
    |      "typeName": "org.apache.gearpump.shared.Messages.Histogram",
    |      "json": "[\"org.apache.gearpump.shared.Messages.Histogram\",{\"name\":\"app1.processor9.task0.receiveLatency\",\"count\":\"0\",\"min\":\"0\",\"max\":\"0\",\"mean\":0,\"stddev\":0,\"median\":0,\"p75\":0,\"p95\":0,\"p98\":0,\"p99\":0,\"p999\":0}]"
    |    }
    |  },
    |  {
    |    "time": "1435595418857",
    |    "value": {
    |      "typeName": "org.apache.gearpump.shared.Messages.Histogram",
    |      "json": "[\"org.apache.gearpump.shared.Messages.Histogram\",{\"name\":\"app1.processor10.task0.receiveLatency\",\"count\":\"0\",\"min\":\"0\",\"max\":\"0\",\"mean\":0,\"stddev\":0,\"median\":0,\"p75\":0,\"p95\":0,\"p98\":0,\"p99\":0,\"p999\":0}]"
    |    }
    |  },
    |  {
    |    "time": "1435595418892",
    |    "value": {
    |      "typeName": "org.apache.gearpump.shared.Messages.Histogram",
    |      "json": "[\"org.apache.gearpump.shared.Messages.Histogram\",{\"name\":\"app1.processor4.task0.processTime\",\"count\":\"3\",\"min\":\"0\",\"max\":\"1\",\"mean\":0.3333333333333333,\"stddev\":0.5773502691896258,\"median\":0,\"p75\":1,\"p95\":1,\"p98\":1,\"p99\":1,\"p999\":1}]"
    |    }
    |  },
    |  {
    |    "time": "1435595418898",
    |    "value": {
    |      "typeName": "org.apache.gearpump.shared.Messages.Meter",
    |      "json": "[\"org.apache.gearpump.shared.Messages.Meter\",{\"name\":\"app1.processor3.task0.sendThroughput\",\"count\":\"10\",\"meanRate\":1.84325000688076,\"m1\":2,\"m5\":2,\"m15\":2,\"rateUnit\":\"events/second\"}]"
    |    }
    |  }
    |]
    |}
  """.stripMargin
  var streamingDag: StreamingDag = _
  val streamingAppMasterDataDetailJSON =
    """
      |{
      |  "appId": 1,
      |  "appName": "dag",
      |  "actorPath": "akka.tcp://app1-executor-1@ip-10-10-10-217.eu-west-1.compute.internal:34731/user/daemon/appdaemon1/$c/appmaster",
      |  "clock": "0",
      |  "executors": [
      |    [
      |      0,
      |      "akka.tcp://app1system0@ip-10-10-10-217.eu-west-1.compute.internal:60598/rem…t-1.compute.internal:34731/user/daemon/appdaemon1/$c/appmaster/executors/0"
      |    ],
      |    [
      |      1,
      |      "akka.tcp://app1system1@ip-10-10-10-217.eu-west-1.compute.internal:53494/rem…t-1.compute.internal:34731/user/daemon/appdaemon1/$c/appmaster/executors/1"
      |    ]
      |  ],
      |  "tasks": [
      |    [
      |      {
      |        "processorId": 5,
      |        "index": 0
      |      },
      |      1
      |    ],
      |    [
      |      {
      |        "processorId": 9,
      |        "index": 0
      |      },
      |      1
      |    ],
      |    [
      |      {
      |        "processorId": 0,
      |        "index": 0
      |      },
      |      1
      |    ],
      |    [
      |      {
      |        "processorId": 4,
      |        "index": 0
      |      },
      |      0
      |    ],
      |    [
      |      {
      |        "processorId": 2,
      |        "index": 0
      |      },
      |      0
      |    ],
      |    [
      |      {
      |        "processorId": 3,
      |        "index": 0
      |      },
      |      0
      |    ],
      |    [
      |      {
      |        "processorId": 8,
      |        "index": 0
      |      },
      |      0
      |    ],
      |    [
      |      {
      |        "processorId": 10,
      |        "index": 0
      |      },
      |      1
      |    ],
      |    [
      |      {
      |        "processorId": 6,
      |        "index": 0
      |      },
      |      1
      |    ],
      |    [
      |      {
      |        "processorId": 11,
      |        "index": 0
      |      },
      |      0
      |    ],
      |    [
      |      {
      |        "processorId": 1,
      |        "index": 0
      |      },
      |      1
      |    ],
      |    [
      |      {
      |        "processorId": 7,
      |        "index": 0
      |      },
      |      0
      |    ]
      |  ],
      |  "processors": [
      |    [
      |      0,
      |      {
      |        "id": 0,
      |        "taskClass": "org.apache.gearpump.streaming.examples.complexdag.Source_0",
      |        "parallelism": 1
      |      }
      |    ],
      |    [
      |      5,
      |      {
      |        "id": 5,
      |        "taskClass": "org.apache.gearpump.streaming.examples.complexdag.Node_1",
      |        "parallelism": 1
      |      }
      |    ],
      |    [
      |      10,
      |      {
      |        "id": 10,
      |        "taskClass": "org.apache.gearpump.streaming.examples.complexdag.Sink_4",
      |        "parallelism": 1
      |      }
      |    ],
      |    [
      |      1,
      |      {
      |        "id": 1,
      |        "taskClass": "org.apache.gearpump.streaming.examples.complexdag.Sink_1",
      |        "parallelism": 1
      |      }
      |    ],
      |    [
      |      6,
      |      {
      |        "id": 6,
      |        "taskClass": "org.apache.gearpump.streaming.examples.complexdag.Sink_0",
      |        "parallelism": 1
      |      }
      |    ],
      |    [
      |      9,
      |      {
      |        "id": 9,
      |        "taskClass": "org.apache.gearpump.streaming.examples.complexdag.Source_1",
      |        "parallelism": 1
      |      }
      |    ],
      |    [
      |      2,
      |      {
      |        "id": 2,
      |        "taskClass": "org.apache.gearpump.streaming.examples.complexdag.Sink_2",
      |        "parallelism": 1
      |      }
      |    ],
      |    [
      |      7,
      |      {
      |        "id": 7,
      |        "taskClass": "org.apache.gearpump.streaming.examples.complexdag.Sink_3",
      |        "parallelism": 1
      |      }
      |    ],
      |    [
      |      3,
      |      {
      |        "id": 3,
      |        "taskClass": "org.apache.gearpump.streaming.examples.complexdag.Node_2",
      |        "parallelism": 1
      |      }
      |    ],
      |    [
      |      11,
      |      {
      |        "id": 11,
      |        "taskClass": "org.apache.gearpump.streaming.examples.complexdag.Node_0",
      |        "parallelism": 1
      |      }
      |    ],
      |    [
      |      8,
      |      {
      |        "id": 8,
      |        "taskClass": "org.apache.gearpump.streaming.examples.complexdag.Node_4",
      |        "parallelism": 1
      |      }
      |    ],
      |    [
      |      4,
      |      {
      |        "id": 4,
      |        "taskClass": "org.apache.gearpump.streaming.examples.complexdag.Node_3",
      |        "parallelism": 1
      |      }
      |    ]
      |  ],
      |  "processorLevels": [
      |    [
      |      0,
      |      0
      |    ],
      |    [
      |      5,
      |      1
      |    ],
      |    [
      |      10,
      |      1
      |    ],
      |    [
      |      1,
      |      1
      |    ],
      |    [
      |      6,
      |      1
      |    ],
      |    [
      |      9,
      |      0
      |    ],
      |    [
      |      2,
      |      1
      |    ],
      |    [
      |      7,
      |      3
      |    ],
      |    [
      |      3,
      |      1
      |    ],
      |    [
      |      11,
      |      1
      |    ],
      |    [
      |      8,
      |      2
      |    ],
      |    [
      |      4,
      |      2
      |    ]
      |  ],
      |  "dag": {
      |    "vertices": [
      |      2,
      |      9,
      |      0,
      |      5,
      |      8,
      |      1,
      |      4,
      |      11,
      |      6,
      |      10,
      |      3,
      |      7
      |    ],
      |    "edges": [
      |      [
      |        8,
      |        "org.apache.gearpump.partitioner.HashPartitioner",
      |        7
      |      ],
      |      [
      |        9,
      |        "org.apache.gearpump.partitioner.HashPartitioner",
      |        11
      |      ],
      |      [
      |        0,
      |        "org.apache.gearpump.partitioner.HashPartitioner",
      |        6
      |      ],
      |      [
      |        4,
      |        "org.apache.gearpump.partitioner.HashPartitioner",
      |        7
      |      ],
      |      [
      |        0,
      |        "org.apache.gearpump.partitioner.HashPartitioner",
      |        3
      |      ],
      |      [
      |        0,
      |        "org.apache.gearpump.partitioner.HashPartitioner",
      |        5
      |      ],
      |      [
      |        11,
      |        "org.apache.gearpump.partitioner.HashPartitioner",
      |        7
      |      ],
      |      [
      |        5,
      |        "org.apache.gearpump.partitioner.HashPartitioner",
      |        7
      |      ],
      |      [
      |        0,
      |        "org.apache.gearpump.partitioner.HashPartitioner",
      |        1
      |      ],
      |      [
      |        3,
      |        "org.apache.gearpump.partitioner.HashPartitioner",
      |        4
      |      ],
      |      [
      |        0,
      |        "org.apache.gearpump.partitioner.HashPartitioner",
      |        2
      |      ],
      |      [
      |        0,
      |        "org.apache.gearpump.partitioner.HashPartitioner",
      |        4
      |      ],
      |      [
      |        5,
      |        "org.apache.gearpump.partitioner.HashPartitioner",
      |        8
      |      ],
      |      [
      |        9,
      |        "org.apache.gearpump.partitioner.HashPartitioner",
      |        10
      |      ],
      |      [
      |        5,
      |        "org.apache.gearpump.partitioner.HashPartitioner",
      |        4
      |      ]
      |    ]
      |  }
      |}
    """.stripMargin
  val tests = TestSuite {
    "test StreamingDag create with StreamingAppMasterDataDetail" - {
      val obj = JSON.parse(streamingAppMasterDataDetailJSON)
      val streamingAppMasterDataDetail = upickle.read[StreamingAppMasterDataDetail](streamingAppMasterDataDetailJSON)
      streamingDag = StreamingDag(streamingAppMasterDataDetail)
      assert(streamingDag.appId == 1)
    }
    "test StreamingDag.updateMetrics" - {
      val historyMetrics = upickle.read[HistoryMetrics](metricsJSON)
      assert(streamingDag.updateMetrics(historyMetrics))
    }
    "test StreamingDag.getReceivedMessages" - {
      val receivedMessagesCorrect = AggregatedProcessedMessages(total=325,rate=18.66112496739520)
      val receivedMessages = streamingDag.getReceivedMessages(undefined)
      assert(receivedMessagesCorrect.total == receivedMessages.total)
      assert(receivedMessagesCorrect.rate == receivedMessages.rate)
    }
    "test StreamingDag.getSentMessages" - {
      val sentMessagesCorrect = AggregatedProcessedMessages(total=240,rate=13.798860449520152)
      val sentMessages = streamingDag.getSentMessages(undefined)
      assert(sentMessagesCorrect.total == sentMessages.total)
      assert(sentMessagesCorrect.rate == sentMessages.rate)
    }
    "test StreamingDag.getProcessingTime" - {
      val processingTimeCorrect = Array(0.06388888888888888)
      val processingTime = streamingDag.getProcessingTime(undefined)
      assert(processingTime.length==1)
      assert(processingTimeCorrect(0)==processingTime(0))
    }
    "test StreamingDag.getReceiveLatency" - {
      val receiveLatencyCorrect = Array(1.7833333333333332)
      val receiveLatency = streamingDag.getReceiveLatency(undefined)
      assert(receiveLatency.length==1)
      assert(receiveLatencyCorrect(0)==receiveLatency(0))
    }
  }
}
