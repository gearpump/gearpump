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
package gearpump.streaming.examples.transport

import gearpump.Message
import gearpump.streaming.examples.transport.generator.{MockCity, PassRecordGenerator}
import gearpump.streaming.task.{Task, TaskContext, TaskId, StartTime}
import gearpump.cluster.UserConfig
import scala.concurrent.duration._

class DataSource(taskContext: TaskContext, conf: UserConfig) extends Task(taskContext, conf){
  import taskContext.{output, parallelism, taskId, scheduleOnce}

  import system.dispatcher
  private val overdriveThreshold = conf.getInt(VelocityInspector.OVER_DRIVE_THRESHOLD).get
  private val vehicleNum = conf.getInt(DataSource.VEHICLE_NUM).get / parallelism
  private val citySize = conf.getInt(DataSource.MOCK_CITY_SIZE).get
  private val mockCity = new MockCity(citySize)
  private val recordGenerators: Array[PassRecordGenerator] = 
    PassRecordGenerator.create(vehicleNum, getIdentifier(taskId), mockCity, overdriveThreshold)
  
  override def onStart(startTime: StartTime): Unit = {
    self ! Message("start", System.currentTimeMillis())
  }

  override def onNext(msg: Message): Unit = {
    recordGenerators.foreach(generator => 
      output(Message(generator.getNextPassRecord(), System.currentTimeMillis())))
    scheduleOnce(1 second)(self ! Message("continue", System.currentTimeMillis()))
  }
  
  private def getIdentifier(taskId: TaskId): String = {
    s"沪A${taskId.processorId}${taskId.index}"
  }
}

object DataSource {
  final val VEHICLE_NUM = "vehicle.number"
  final val MOCK_CITY_SIZE = "mock.city.size"
}
