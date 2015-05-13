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
package gearpump.streaming.example.transport

import gearpump.Message
import gearpump.streaming.MockUtil
import gearpump.streaming.examples.transport.{DataSource, VelocityInspector}
import gearpump.streaming.task.StartTime
import gearpump.cluster.UserConfig
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.{Matchers, FlatSpec}

class DataSourceSpec extends FlatSpec with Matchers {
  it should "create the pass record" in {
    val vehicleNum = 2
    val context = MockUtil.mockTaskContext

    val userConfig = UserConfig.empty.withInt(DataSource.VEHICLE_NUM, vehicleNum).
      withInt(DataSource.MOCK_CITY_SIZE, 10).
      withInt(VelocityInspector.OVER_DRIVE_THRESHOLD, 60).
      withInt(VelocityInspector.FAKE_PLATE_THRESHOLD, 200)

    val source = new DataSource(context, userConfig)
    source.onStart(StartTime(0))
    source.onNext(Message("start"))
    verify(context, times(vehicleNum)).output(any[Message])
    source.onStop()
  }
}
