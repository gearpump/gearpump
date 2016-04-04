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
package io.gearpump.streaming.appmaster

import com.typesafe.config.{ConfigValueFactory, ConfigFactory, ConfigRenderOptions, Config}
import TaskLocator.{Localities, WorkerLocality, NonLocality, Locality}
import io.gearpump.WorkerId
import io.gearpump.streaming.Constants
import io.gearpump.streaming.task.TaskId
import scala.util.Try
import scala.collection.JavaConverters._

/**
 * TaskLocator is used to decide which machine one task should run on.
 *
 * User can specify config [[Constants.GEARPUMP_STREAMING_LOCALITIES]] to decide
 * to control which machine the task is running on.
 */
class TaskLocator(appName: String, config: Config) {
  private val taskLocalities: Map[TaskId, Locality] = loadTaskLocalities(config)

  def locateTask(taskId: TaskId) : Locality = {
    taskLocalities.getOrElse(taskId, NonLocality)
  }

  private def loadTaskLocalities(config: Config) : Map[TaskId, Locality] = {
    import Constants.GEARPUMP_STREAMING_LOCALITIES
    Try(config.getConfig(s"$GEARPUMP_STREAMING_LOCALITIES.$appName")).map {appConfig =>
      val json = appConfig.root().render(ConfigRenderOptions.concise)
      Localities.fromJson(json)
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

  case class WorkerLocality(workerId: WorkerId) extends Locality

  object NonLocality extends Locality

  case class Localities(localities: Map[WorkerId, Array[TaskId]])

  object Localities {
    val pattern = "task_([0-9]+)_([0-9]+)".r

    def fromJson(json: String): Localities = {
      val localities = ConfigFactory.parseString(json).getAnyRef("localities")
        .asInstanceOf[java.util.Map[String, String]].asScala.map { pair =>
        val workerId: WorkerId = WorkerId.parse(pair._1)
        val tasks = pair._2.split(",").map { task =>
          val pattern(processorId, taskIndex) = task
          TaskId(processorId.toInt, taskIndex.toInt)
        }
        (workerId, tasks)
      }.toMap
      new Localities(localities)
    }

    def toJson(localities: Localities): String = {
      val map = localities.localities.toList.map {pair =>
        (WorkerId.render(pair._1), pair._2.map(task => s"task_${task.processorId}_${task.index}").mkString(","))
      }.toMap.asJava
      ConfigFactory.empty().withValue("localities", ConfigValueFactory.fromAnyRef(map)).
        root.render(ConfigRenderOptions.concise())
    }
  }
}