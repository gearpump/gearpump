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
package org.apache.gearpump.streaming.appmaster

import java.util

import akka.actor.Actor
import com.typesafe.config.{ConfigRenderOptions, Config}
import org.apache.gearpump.cluster.ClusterConfig
import org.apache.gearpump.streaming.{ProcessorId, ProcessorDescription}
import org.apache.gearpump.streaming.appmaster.TaskLocator.{Localities, WorkerLocality, NonLocality, Locality}
import org.apache.gearpump.streaming.task.{TaskId, Task, TaskUtil}
import org.apache.gearpump.util.{ActorUtil, Constants}

import scala.collection.mutable
import scala.util.Try

class TaskLocator(appName: String, config: Config) {
  private var taskLocalities: Map[TaskId, Locality] = loadTaskLocalities(config)

  def locateTask(taskId: TaskId) : Locality = {
    taskLocalities.getOrElse(taskId, NonLocality)
  }

  /*
The task resource requests format:
gearpump {
  streaming {
    localities {
      app1: {
        workerId: [
          TaskId(0,0), TaskId(0,1)
        ]
      }
      app2: {
        workerId: [
          TaskId(1,0), TaskId(1,1)
        ]
      }
    }
  }
}
 */
  def loadTaskLocalities(config: Config) : Map[TaskId, Locality] = {
    import org.apache.gearpump.streaming.Constants.GEARPUMP_STREAMING_LOCALITIES
    Try(config.getConfig(s"$GEARPUMP_STREAMING_LOCALITIES.$appName")).map {appConfig =>
      val json = appConfig.root().render(ConfigRenderOptions.concise)
      import upickle._
      upickle.read[Localities](json)
    }.map { localityConfig =>
      import localityConfig.localities
      localities.keySet.flatMap {workerId =>
        val tasks = localities(workerId)
        tasks.map((_, WorkerLocality(workerId)))
      }.toArray.toMap
    }.getOrElse(Map.empty[TaskId, Locality])
  }
}

object TaskLocator {

  trait Locality

  case class WorkerLocality(workerId: Int) extends Locality

  object NonLocality extends Locality

  type WorkerId = Int

  case class Localities(localities: Map[WorkerId, Array[TaskId]])
}