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

import org.apache.gearpump.shared.Messages.{HistoryMetrics, StreamingAppMasterDataDetail}
import utest._

import scala.scalajs.js.{JSON, undefined}

object AppMasterCtrlSpec extends TestSuite {
  implicit val ec = utest.ExecutionContext.RunNow

val metricsJSON =
  """
    |{
    |"appId":1,
    |"path":"path",
    |"metrics":
    |[
    |  {
    |    "time": "1435680269854",
    |    "value": {
    |      "typeName": "org.apache.gearpump.shared.Messages.Histogram",
    |      "json": "[\"org.apache.gearpump.shared.Messages.Histogram\",{\"name\":\"app1.processor8.task0.receiveLatency\",\"count\":\"9\",\"min\":\"0\",\"max\":\"8\",\"mean\":4.333333333333333,\"stddev\":2.8722813232690143,\"median\":5,\"p75\":6.5,\"p95\":8,\"p98\":8,\"p99\":8,\"p999\":8}]"
    |    }
    |  },
    |  {
    |    "time": "1435680269854",
    |    "value": {
    |      "typeName": "org.apache.gearpump.shared.Messages.Meter",
    |      "json": "[\"org.apache.gearpump.shared.Messages.Meter\",{\"name\":\"app1.processor4.task0.receiveThroughput\",\"count\":\"543\",\"meanRate\":5.877119779976574,\"m1\":5.856523562730274,\"m5\":5.68403753372094,\"m15\":5.630729887535754,\"rateUnit\":\"events/second\"}]"
    |    }
    |  },
    |  {
    |    "time": "1435680269809",
    |    "value": {
    |      "typeName": "org.apache.gearpump.shared.Messages.Meter",
    |      "json": "[\"org.apache.gearpump.shared.Messages.Meter\",{\"name\":\"app1.processor0.task0.sendThroughput\",\"count\":\"1080\",\"meanRate\":11.685907044186779,\"m1\":11.040532975551878,\"m5\":11.80165744585941,\"m15\":11.933518176058762,\"rateUnit\":\"events/second\"}]"
    |    }
    |  },
    |  {
    |    "time": "1435680269854",
    |    "value": {
    |      "typeName": "org.apache.gearpump.shared.Messages.Histogram",
    |      "json": "[\"org.apache.gearpump.shared.Messages.Histogram\",{\"name\":\"app1.processor7.task0.processTime\",\"count\":\"110\",\"min\":\"0\",\"max\":\"0\",\"mean\":0,\"stddev\":0,\"median\":0,\"p75\":0,\"p95\":0,\"p98\":0,\"p99\":0,\"p999\":0}]"
    |    }
    |  },
    |  {
    |    "time": "1435680269809",
    |    "value": {
    |      "typeName": "org.apache.gearpump.shared.Messages.Meter",
    |      "json": "[\"org.apache.gearpump.shared.Messages.Meter\",{\"name\":\"app1.processor10.task0.sendThroughput\",\"count\":\"0\",\"meanRate\":0,\"m1\":0,\"m5\":0,\"m15\":0,\"rateUnit\":\"events/second\"}]"
    |    }
    |  },
    |  {
    |    "time": "1435680269854",
    |    "value": {
    |      "typeName": "org.apache.gearpump.shared.Messages.Meter",
    |      "json": "[\"org.apache.gearpump.shared.Messages.Meter\",{\"name\":\"app1.processor11.task0.receiveThroughput\",\"count\":\"177\",\"meanRate\":1.9157485123820237,\"m1\":1.8940679647982497,\"m5\":1.6957508470291223,\"m15\":1.6349829197598136,\"rateUnit\":\"events/second\"}]"
    |    }
    |  },
    |  {
    |    "time": "1435680269855",
    |    "value": {
    |      "typeName": "org.apache.gearpump.shared.Messages.Meter",
    |      "json": "[\"org.apache.gearpump.shared.Messages.Meter\",{\"name\":\"app1.processor7.task0.sendThroughput\",\"count\":\"0\",\"meanRate\":0,\"m1\":0,\"m5\":0,\"m15\":0,\"rateUnit\":\"events/second\"}]"
    |    }
    |  },
    |  {
    |    "time": "1435680269854",
    |    "value": {
    |      "typeName": "org.apache.gearpump.shared.Messages.Histogram",
    |      "json": "[\"org.apache.gearpump.shared.Messages.Histogram\",{\"name\":\"app1.processor4.task0.receiveLatency\",\"count\":\"27\",\"min\":\"0\",\"max\":\"8\",\"mean\":2.2222222222222223,\"stddev\":2.1363760352762253,\"median\":3,\"p75\":4,\"p95\":6.799999999999994,\"p98\":8,\"p99\":8,\"p999\":8}]"
    |    }
    |  },
    |  {
    |    "time": "1435680269809",
    |    "value": {
    |      "typeName": "org.apache.gearpump.shared.Messages.Meter",
    |      "json": "[\"org.apache.gearpump.shared.Messages.Meter\",{\"name\":\"app1.processor5.task0.sendThroughput\",\"count\":\"540\",\"meanRate\":5.8429494984317945,\"m1\":5.520266487775939,\"m5\":5.900828722929705,\"m15\":5.966759088029381,\"rateUnit\":\"events/second\"}]"
    |    }
    |  },
    |  {
    |    "time": "1435680269854",
    |    "value": {
    |      "typeName": "org.apache.gearpump.shared.Messages.Histogram",
    |      "json": "[\"org.apache.gearpump.shared.Messages.Histogram\",{\"name\":\"app1.processor7.task0.receiveLatency\",\"count\":\"36\",\"min\":\"0\",\"max\":\"1\",\"mean\":0.1111111111111111,\"stddev\":0.3187276291558383,\"median\":0,\"p75\":0,\"p95\":1,\"p98\":1,\"p99\":1,\"p999\":1}]"
    |    }
    |  },
    |  {
    |    "time": "1435680269808",
    |    "value": {
    |      "typeName": "org.apache.gearpump.shared.Messages.Histogram",
    |      "json": "[\"org.apache.gearpump.shared.Messages.Histogram\",{\"name\":\"app1.processor9.task0.processTime\",\"count\":\"18\",\"min\":\"0\",\"max\":\"1\",\"mean\":0.2777777777777778,\"stddev\":0.46088859896247675,\"median\":0,\"p75\":1,\"p95\":1,\"p98\":1,\"p99\":1,\"p999\":1}]"
    |    }
    |  },
    |  {
    |    "time": "1435680269807",
    |    "value": {
    |      "typeName": "org.apache.gearpump.shared.Messages.Histogram",
    |      "json": "[\"org.apache.gearpump.shared.Messages.Histogram\",{\"name\":\"app1.processor0.task0.receiveLatency\",\"count\":\"0\",\"min\":\"0\",\"max\":\"0\",\"mean\":0,\"stddev\":0,\"median\":0,\"p75\":0,\"p95\":0,\"p98\":0,\"p99\":0,\"p999\":0}]"
    |    }
    |  },
    |  {
    |    "time": "1435680269853",
    |    "value": {
    |      "typeName": "org.apache.gearpump.shared.Messages.Histogram",
    |      "json": "[\"org.apache.gearpump.shared.Messages.Histogram\",{\"name\":\"app1.processor3.task0.processTime\",\"count\":\"18\",\"min\":\"0\",\"max\":\"0\",\"mean\":0,\"stddev\":0,\"median\":0,\"p75\":0,\"p95\":0,\"p98\":0,\"p99\":0,\"p999\":0}]"
    |    }
    |  },
    |  {
    |    "time": "1435680269807",
    |    "value": {
    |      "typeName": "org.apache.gearpump.shared.Messages.Histogram",
    |      "json": "[\"org.apache.gearpump.shared.Messages.Histogram\",{\"name\":\"app1.processor0.task0.processTime\",\"count\":\"18\",\"min\":\"0\",\"max\":\"1\",\"mean\":0.3888888888888889,\"stddev\":0.5016313257045503,\"median\":0,\"p75\":1,\"p95\":1,\"p98\":1,\"p99\":1,\"p999\":1}]"
    |    }
    |  },
    |  {
    |    "time": "1435680269855",
    |    "value": {
    |      "typeName": "org.apache.gearpump.shared.Messages.Meter",
    |      "json": "[\"org.apache.gearpump.shared.Messages.Meter\",{\"name\":\"app1.processor8.task0.receiveThroughput\",\"count\":\"177\",\"meanRate\":1.9157529806365892,\"m1\":1.8940679647982497,\"m5\":1.6957508470291223,\"m15\":1.6349829197598136,\"rateUnit\":\"events/second\"}]"
    |    }
    |  },
    |  {
    |    "time": "1435680269855",
    |    "value": {
    |      "typeName": "org.apache.gearpump.shared.Messages.Meter",
    |      "json": "[\"org.apache.gearpump.shared.Messages.Meter\",{\"name\":\"app1.processor8.task0.sendThroughput\",\"count\":\"180\",\"meanRate\":1.9482226556644775,\"m1\":2,\"m5\":2,\"m15\":2,\"rateUnit\":\"events/second\"}]"
    |    }
    |  },
    |  {
    |    "time": "1435680269854",
    |    "value": {
    |      "typeName": "org.apache.gearpump.shared.Messages.Meter",
    |      "json": "[\"org.apache.gearpump.shared.Messages.Meter\",{\"name\":\"app1.processor3.task0.receiveThroughput\",\"count\":\"177\",\"meanRate\":1.9158306332026798,\"m1\":1.8940679647982497,\"m5\":1.6957508470291223,\"m15\":1.6349829197598136,\"rateUnit\":\"events/second\"}]"
    |    }
    |  },
    |  {
    |    "time": "1435680269808",
    |    "value": {
    |      "typeName": "org.apache.gearpump.shared.Messages.Histogram",
    |      "json": "[\"org.apache.gearpump.shared.Messages.Histogram\",{\"name\":\"app1.processor1.task0.processTime\",\"count\":\"18\",\"min\":\"0\",\"max\":\"0\",\"mean\":0,\"stddev\":0,\"median\":0,\"p75\":0,\"p95\":0,\"p98\":0,\"p99\":0,\"p999\":0}]"
    |    }
    |  },
    |  {
    |    "time": "1435680269809",
    |    "value": {
    |      "typeName": "org.apache.gearpump.shared.Messages.Meter",
    |      "json": "[\"org.apache.gearpump.shared.Messages.Meter\",{\"name\":\"app1.processor9.task0.receiveThroughput\",\"count\":\"175\",\"meanRate\":1.8935497514648503,\"m1\":1.8366401445236284,\"m5\":1.5421554253491752,\"m15\":1.451941495922701,\"rateUnit\":\"events/second\"}]"
    |    }
    |  },
    |  {
    |    "time": "1435680269809",
    |    "value": {
    |      "typeName": "org.apache.gearpump.shared.Messages.Meter",
    |      "json": "[\"org.apache.gearpump.shared.Messages.Meter\",{\"name\":\"app1.processor9.task0.sendThroughput\",\"count\":\"360\",\"meanRate\":3.89527491882991,\"m1\":3.6801776585172936,\"m5\":3.9338858152864704,\"m15\":3.9778393920195874,\"rateUnit\":\"events/second\"}]"
    |    }
    |  },
    |  {
    |    "time": "1435680269809",
    |    "value": {
    |      "typeName": "org.apache.gearpump.shared.Messages.Meter",
    |      "json": "[\"org.apache.gearpump.shared.Messages.Meter\",{\"name\":\"app1.processor1.task0.receiveThroughput\",\"count\":\"179\",\"meanRate\":1.9368250762178074,\"m1\":1.9514957850728702,\"m5\":1.8493462687090683,\"m15\":1.8180243435969263,\"rateUnit\":\"events/second\"}]"
    |    }
    |  },
    |  {
    |    "time": "1435680269808",
    |    "value": {
    |      "typeName": "org.apache.gearpump.shared.Messages.Histogram",
    |      "json": "[\"org.apache.gearpump.shared.Messages.Histogram\",{\"name\":\"app1.processor5.task0.processTime\",\"count\":\"18\",\"min\":\"0\",\"max\":\"1\",\"mean\":0.2222222222222222,\"stddev\":0.42779263194649864,\"median\":0,\"p75\":0.25,\"p95\":1,\"p98\":1,\"p99\":1,\"p999\":1}]"
    |    }
    |  },
    |  {
    |    "time": "1435680269808",
    |    "value": {
    |      "typeName": "org.apache.gearpump.shared.Messages.Meter",
    |      "json": "[\"org.apache.gearpump.shared.Messages.Meter\",{\"name\":\"app1.processor0.task0.receiveThroughput\",\"count\":\"177\",\"meanRate\":1.9151912073045547,\"m1\":1.6961649169000554,\"m5\":0.9571906425195416,\"m15\":0.7304334749146412,\"rateUnit\":\"events/second\"}]"
    |    }
    |  },
    |  {
    |    "time": "1435680269854",
    |    "value": {
    |      "typeName": "org.apache.gearpump.shared.Messages.Meter",
    |      "json": "[\"org.apache.gearpump.shared.Messages.Meter\",{\"name\":\"app1.processor2.task0.sendThroughput\",\"count\":\"0\",\"meanRate\":0,\"m1\":0,\"m5\":0,\"m15\":0,\"rateUnit\":\"events/second\"}]"
    |    }
    |  },
    |  {
    |    "time": "1435680269855",
    |    "value": {
    |      "typeName": "org.apache.gearpump.shared.Messages.Meter",
    |      "json": "[\"org.apache.gearpump.shared.Messages.Meter\",{\"name\":\"app1.processor7.task0.receiveThroughput\",\"count\":\"1099\",\"meanRate\":11.894826925806804,\"m1\":11.951495785072868,\"m5\":11.849346268709066,\"m15\":11.818024343596928,\"rateUnit\":\"events/second\"}]"
    |    }
    |  },
    |  {
    |    "time": "1435680269854",
    |    "value": {
    |      "typeName": "org.apache.gearpump.shared.Messages.Meter",
    |      "json": "[\"org.apache.gearpump.shared.Messages.Meter\",{\"name\":\"app1.processor11.task0.sendThroughput\",\"count\":\"180\",\"meanRate\":1.9482165485668792,\"m1\":2,\"m5\":2,\"m15\":2,\"rateUnit\":\"events/second\"}]"
    |    }
    |  },
    |  {
    |    "time": "1435680269808",
    |    "value": {
    |      "typeName": "org.apache.gearpump.shared.Messages.Histogram",
    |      "json": "[\"org.apache.gearpump.shared.Messages.Histogram\",{\"name\":\"app1.processor5.task0.receiveLatency\",\"count\":\"9\",\"min\":\"0\",\"max\":\"1\",\"mean\":0.1111111111111111,\"stddev\":0.3333333333333333,\"median\":0,\"p75\":0,\"p95\":1,\"p98\":1,\"p99\":1,\"p999\":1}]"
    |    }
    |  },
    |  {
    |    "time": "1435680269853",
    |    "value": {
    |      "typeName": "org.apache.gearpump.shared.Messages.Histogram",
    |      "json": "[\"org.apache.gearpump.shared.Messages.Histogram\",{\"name\":\"app1.processor11.task0.processTime\",\"count\":\"18\",\"min\":\"0\",\"max\":\"0\",\"mean\":0,\"stddev\":0,\"median\":0,\"p75\":0,\"p95\":0,\"p98\":0,\"p99\":0,\"p999\":0}]"
    |    }
    |  },
    |  {
    |    "time": "1435680269854",
    |    "value": {
    |      "typeName": "org.apache.gearpump.shared.Messages.Histogram",
    |      "json": "[\"org.apache.gearpump.shared.Messages.Histogram\",{\"name\":\"app1.processor8.task0.processTime\",\"count\":\"18\",\"min\":\"0\",\"max\":\"0\",\"mean\":0,\"stddev\":0,\"median\":0,\"p75\":0,\"p95\":0,\"p98\":0,\"p99\":0,\"p999\":0}]"
    |    }
    |  },
    |  {
    |    "time": "1435680269808",
    |    "value": {
    |      "typeName": "org.apache.gearpump.shared.Messages.Histogram",
    |      "json": "[\"org.apache.gearpump.shared.Messages.Histogram\",{\"name\":\"app1.processor6.task0.receiveLatency\",\"count\":\"9\",\"min\":\"0\",\"max\":\"1\",\"mean\":0.1111111111111111,\"stddev\":0.3333333333333333,\"median\":0,\"p75\":0,\"p95\":1,\"p98\":1,\"p99\":1,\"p999\":1}]"
    |    }
    |  },
    |  {
    |    "time": "1435680269808",
    |    "value": {
    |      "typeName": "org.apache.gearpump.shared.Messages.Histogram",
    |      "json": "[\"org.apache.gearpump.shared.Messages.Histogram\",{\"name\":\"app1.processor10.task0.processTime\",\"count\":\"18\",\"min\":\"0\",\"max\":\"0\",\"mean\":0,\"stddev\":0,\"median\":0,\"p75\":0,\"p95\":0,\"p98\":0,\"p99\":0,\"p999\":0}]"
    |    }
    |  },
    |  {
    |    "time": "1435680269854",
    |    "value": {
    |      "typeName": "org.apache.gearpump.shared.Messages.Meter",
    |      "json": "[\"org.apache.gearpump.shared.Messages.Meter\",{\"name\":\"app1.processor4.task0.sendThroughput\",\"count\":\"550\",\"meanRate\":5.952878402538633,\"m1\":6,\"m5\":6,\"m15\":6,\"rateUnit\":\"events/second\"}]"
    |    }
    |  },
    |  {
    |    "time": "1435680269809",
    |    "value": {
    |      "typeName": "org.apache.gearpump.shared.Messages.Meter",
    |      "json": "[\"org.apache.gearpump.shared.Messages.Meter\",{\"name\":\"app1.processor6.task0.sendThroughput\",\"count\":\"0\",\"meanRate\":0,\"m1\":0,\"m5\":0,\"m15\":0,\"rateUnit\":\"events/second\"}]"
    |    }
    |  },
    |  {
    |    "time": "1435680269854",
    |    "value": {
    |      "typeName": "org.apache.gearpump.shared.Messages.Meter",
    |      "json": "[\"org.apache.gearpump.shared.Messages.Meter\",{\"name\":\"app1.processor2.task0.receiveThroughput\",\"count\":\"179\",\"meanRate\":1.937398060263143,\"m1\":1.9514957850728702,\"m5\":1.8493462687090683,\"m15\":1.8180243435969263,\"rateUnit\":\"events/second\"}]"
    |    }
    |  },
    |  {
    |    "time": "1435680269809",
    |    "value": {
    |      "typeName": "org.apache.gearpump.shared.Messages.Meter",
    |      "json": "[\"org.apache.gearpump.shared.Messages.Meter\",{\"name\":\"app1.processor1.task0.sendThroughput\",\"count\":\"0\",\"meanRate\":0,\"m1\":0,\"m5\":0,\"m15\":0,\"rateUnit\":\"events/second\"}]"
    |    }
    |  },
    |  {
    |    "time": "1435680269808",
    |    "value": {
    |      "typeName": "org.apache.gearpump.shared.Messages.Histogram",
    |      "json": "[\"org.apache.gearpump.shared.Messages.Histogram\",{\"name\":\"app1.processor6.task0.processTime\",\"count\":\"18\",\"min\":\"0\",\"max\":\"1\",\"mean\":0.05555555555555555,\"stddev\":0.23570226039551584,\"median\":0,\"p75\":0,\"p95\":1,\"p98\":1,\"p99\":1,\"p999\":1}]"
    |    }
    |  },
    |  {
    |    "time": "1435680269853",
    |    "value": {
    |      "typeName": "org.apache.gearpump.shared.Messages.Histogram",
    |      "json": "[\"org.apache.gearpump.shared.Messages.Histogram\",{\"name\":\"app1.processor11.task0.receiveLatency\",\"count\":\"9\",\"min\":\"1\",\"max\":\"8\",\"mean\":4.777777777777778,\"stddev\":2.279132388529557,\"median\":5,\"p75\":6.5,\"p95\":8,\"p98\":8,\"p99\":8,\"p999\":8}]"
    |    }
    |  },
    |  {
    |    "time": "1435680269853",
    |    "value": {
    |      "typeName": "org.apache.gearpump.shared.Messages.Histogram",
    |      "json": "[\"org.apache.gearpump.shared.Messages.Histogram\",{\"name\":\"app1.processor2.task0.receiveLatency\",\"count\":\"9\",\"min\":\"2\",\"max\":\"8\",\"mean\":5,\"stddev\":1.9364916731037085,\"median\":5,\"p75\":6.5,\"p95\":8,\"p98\":8,\"p99\":8,\"p999\":8}]"
    |    }
    |  },
    |  {
    |    "time": "1435680269808",
    |    "value": {
    |      "typeName": "org.apache.gearpump.shared.Messages.Histogram",
    |      "json": "[\"org.apache.gearpump.shared.Messages.Histogram\",{\"name\":\"app1.processor1.task0.receiveLatency\",\"count\":\"9\",\"min\":\"0\",\"max\":\"1\",\"mean\":0.1111111111111111,\"stddev\":0.3333333333333333,\"median\":0,\"p75\":0,\"p95\":1,\"p98\":1,\"p99\":1,\"p999\":1}]"
    |    }
    |  },
    |  {
    |    "time": "1435680269809",
    |    "value": {
    |      "typeName": "org.apache.gearpump.shared.Messages.Meter",
    |      "json": "[\"org.apache.gearpump.shared.Messages.Meter\",{\"name\":\"app1.processor5.task0.receiveThroughput\",\"count\":\"183\",\"meanRate\":1.9801115372762477,\"m1\":1.7792123242490079,\"m5\":1.388560003669229,\"m15\":1.2689000720855887,\"rateUnit\":\"events/second\"}]"
    |    }
    |  },
    |  {
    |    "time": "1435680269853",
    |    "value": {
    |      "typeName": "org.apache.gearpump.shared.Messages.Histogram",
    |      "json": "[\"org.apache.gearpump.shared.Messages.Histogram\",{\"name\":\"app1.processor3.task0.receiveLatency\",\"count\":\"9\",\"min\":\"2\",\"max\":\"8\",\"mean\":5,\"stddev\":1.9364916731037085,\"median\":5,\"p75\":6.5,\"p95\":8,\"p98\":8,\"p99\":8,\"p999\":8}]"
    |    }
    |  },
    |  {
    |    "time": "1435680269853",
    |    "value": {
    |      "typeName": "org.apache.gearpump.shared.Messages.Histogram",
    |      "json": "[\"org.apache.gearpump.shared.Messages.Histogram\",{\"name\":\"app1.processor2.task0.processTime\",\"count\":\"18\",\"min\":\"0\",\"max\":\"1\",\"mean\":0.05555555555555555,\"stddev\":0.23570226039551584,\"median\":0,\"p75\":0,\"p95\":1,\"p98\":1,\"p99\":1,\"p999\":1}]"
    |    }
    |  },
    |  {
    |    "time": "1435680269809",
    |    "value": {
    |      "typeName": "org.apache.gearpump.shared.Messages.Meter",
    |      "json": "[\"org.apache.gearpump.shared.Messages.Meter\",{\"name\":\"app1.processor6.task0.receiveThroughput\",\"count\":\"179\",\"meanRate\":1.9368198282592202,\"m1\":1.9514957850728702,\"m5\":1.8493462687090683,\"m15\":1.8180243435969263,\"rateUnit\":\"events/second\"}]"
    |    }
    |  },
    |  {
    |    "time": "1435680269809",
    |    "value": {
    |      "typeName": "org.apache.gearpump.shared.Messages.Meter",
    |      "json": "[\"org.apache.gearpump.shared.Messages.Meter\",{\"name\":\"app1.processor10.task0.receiveThroughput\",\"count\":\"179\",\"meanRate\":1.9368249416742938,\"m1\":1.9514957850728702,\"m5\":1.8493462687090683,\"m15\":1.8180243435969263,\"rateUnit\":\"events/second\"}]"
    |    }
    |  },
    |  {
    |    "time": "1435680269808",
    |    "value": {
    |      "typeName": "org.apache.gearpump.shared.Messages.Histogram",
    |      "json": "[\"org.apache.gearpump.shared.Messages.Histogram\",{\"name\":\"app1.processor9.task0.receiveLatency\",\"count\":\"0\",\"min\":\"0\",\"max\":\"0\",\"mean\":0,\"stddev\":0,\"median\":0,\"p75\":0,\"p95\":0,\"p98\":0,\"p99\":0,\"p999\":0}]"
    |    }
    |  },
    |  {
    |    "time": "1435680269808",
    |    "value": {
    |      "typeName": "org.apache.gearpump.shared.Messages.Histogram",
    |      "json": "[\"org.apache.gearpump.shared.Messages.Histogram\",{\"name\":\"app1.processor10.task0.receiveLatency\",\"count\":\"9\",\"min\":\"0\",\"max\":\"0\",\"mean\":0,\"stddev\":0,\"median\":0,\"p75\":0,\"p95\":0,\"p98\":0,\"p99\":0,\"p999\":0}]"
    |    }
    |  },
    |  {
    |    "time": "1435680269853",
    |    "value": {
    |      "typeName": "org.apache.gearpump.shared.Messages.Histogram",
    |      "json": "[\"org.apache.gearpump.shared.Messages.Histogram\",{\"name\":\"app1.processor4.task0.processTime\",\"count\":\"55\",\"min\":\"0\",\"max\":\"1\",\"mean\":0.03636363636363636,\"stddev\":0.18891859540615819,\"median\":0,\"p75\":0,\"p95\":0.19999999999999574,\"p98\":1,\"p99\":1,\"p999\":1}]"
    |    }
    |  },
    |  {
    |    "time": "1435680269854",
    |    "value": {
    |      "typeName": "org.apache.gearpump.shared.Messages.Meter",
    |      "json": "[\"org.apache.gearpump.shared.Messages.Meter\",{\"name\":\"app1.processor3.task0.sendThroughput\",\"count\":\"180\",\"meanRate\":1.9483014120438555,\"m1\":2,\"m5\":2,\"m15\":2,\"rateUnit\":\"events/second\"}]"
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
      |  "actorPath": "akka.tcp://app1-executor-1@ip-10-10-10-217.eu-west-1.compute.internal:55144/user/daemon/appdaemon1/$c/appmaster",
      |  "clock": "1435680177628",
      |  "executors": [
      |    [
      |      0,
      |      "akka.tcp://app1system0@ip-10-10-10-217.eu-west-1.compute.internal:52627/rem…t-1.compute.internal:55144/user/daemon/appdaemon1/$c/appmaster/executors/0"
      |    ],
      |    [
      |      1,
      |      "akka.tcp://app1system1@ip-10-10-10-217.eu-west-1.compute.internal:50934/rem…t-1.compute.internal:55144/user/daemon/appdaemon1/$c/appmaster/executors/1"
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
      |      0,
      |      2,
      |      9,
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
      |        0,
      |        "org.apache.gearpump.partitioner.HashPartitioner",
      |        4
      |      ],
      |      [
      |        0,
      |        "org.apache.gearpump.partitioner.HashPartitioner",
      |        2
      |      ],
      |      [
      |        9,
      |        "org.apache.gearpump.partitioner.HashPartitioner",
      |        11
      |      ],
      |      [
      |        5,
      |        "org.apache.gearpump.partitioner.HashPartitioner",
      |        8
      |      ],
      |      [
      |        0,
      |        "org.apache.gearpump.partitioner.HashPartitioner",
      |        1
      |      ],
      |      [
      |        0,
      |        "org.apache.gearpump.partitioner.HashPartitioner",
      |        5
      |      ],
      |      [
      |        3,
      |        "org.apache.gearpump.partitioner.HashPartitioner",
      |        4
      |      ],
      |      [
      |        0,
      |        "org.apache.gearpump.partitioner.HashPartitioner",
      |        6
      |      ],
      |      [
      |        9,
      |        "org.apache.gearpump.partitioner.HashPartitioner",
      |        10
      |      ],
      |      [
      |        8,
      |        "org.apache.gearpump.partitioner.HashPartitioner",
      |        7
      |      ],
      |      [
      |        5,
      |        "org.apache.gearpump.partitioner.HashPartitioner",
      |        4
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
      |        3
      |      ],
      |      [
      |        4,
      |        "org.apache.gearpump.partitioner.HashPartitioner",
      |        7
      |      ]
      |    ]
      |  }
      |}
    """.stripMargin
  val tests = TestSuite {
    "test StreamingDag create with StreamingAppMasterDataDetail" - {
      val streamingAppMasterDataDetail = upickle.read[StreamingAppMasterDataDetail](streamingAppMasterDataDetailJSON)
      streamingDag = StreamingDag(streamingAppMasterDataDetail)
      assert(streamingDag.appId == 1)
    }
    "test StreamingDag.updateMetrics" - {
      val historyMetrics = upickle.read[HistoryMetrics](metricsJSON)
      assert(streamingDag.updateMetrics(historyMetrics))
    }
    "test StreamingDag.getReceivedMessages" - {
      val receivedMessagesCorrect = AggregatedProcessedMessages(total = 1815, rate = 19.642694832221267)
      val receivedMessages = streamingDag.getReceivedMessages(undefined)
      assert(receivedMessagesCorrect.total == receivedMessages.total)
      assert(receivedMessagesCorrect.rate == receivedMessages.rate)
    }
    "test StreamingDag.getSentMessages" - {
      val sentMessagesCorrect = AggregatedProcessedMessages(total = 1440, rate = 15.58118196301669)
      val sentMessages = streamingDag.getSentMessages(undefined)
      assert(sentMessagesCorrect.total == sentMessages.total)
      assert(sentMessagesCorrect.rate == sentMessages.rate)
    }
    "test StreamingDag.getProcessingTime" - {
      val processingTimeCorrect = Array(0.08636363636363636)
      val processingTime = streamingDag.getProcessingTime(undefined)
      assert(processingTime.length == 1)
      assert(processingTimeCorrect(0) == processingTime(0))
    }
    "test StreamingDag.getReceiveLatency" - {
      val receiveLatencyCorrect = Array(1.8148148148148147)
      val receiveLatency = streamingDag.getReceiveLatency(undefined)
      assert(receiveLatency.length == 1)
      assert(receiveLatencyCorrect(0) == receiveLatency(0))
    }
  }
  tests.run().toSeq.foreach(println)

}
