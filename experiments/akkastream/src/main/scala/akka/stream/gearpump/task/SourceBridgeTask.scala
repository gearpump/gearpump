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

package akka.stream.gearpump.task

import akka.actor.Actor.Receive
import akka.actor.ActorSystem
import akka.stream.gearpump.task.SourceBridgeTask.{AkkaStreamMessage, Complete, Error}
import io.gearpump.Message
import io.gearpump.cluster.UserConfig
import io.gearpump.cluster.client.ClientContext
import io.gearpump.streaming.ProcessorId
import io.gearpump.streaming.appmaster.AppMaster.{LookupTaskActorRef, TaskActorRef}
import io.gearpump.streaming.task.{StartTime, Task, TaskContext, TaskId}
import org.reactivestreams.{Subscriber, Subscription}

import scala.concurrent.ExecutionContext

/**
 * Bridge Task when data flow is from local Akka-Stream Module to remote Gearpump Task
 *
 *
 *
 *      [[SourceBridgeTask]]   --> downstream [[Task]]
 *                 /|                Remote Cluster
 * ---------------/--------------------------------
 *               /                    Local JVM
 *    Akka Stream [[org.reactivestreams.Publisher]]
 *
 *
 * @param taskContext
 * @param userConf
 */
class SourceBridgeTask(taskContext : TaskContext, userConf : UserConfig) extends Task(taskContext, userConf) {
  import taskContext.taskId

  override def onStart(startTime : StartTime) : Unit = {}

  override def onNext(msg : Message) : Unit = {
    LOG.info("AkkaStreamSource receiving message " + msg)
  }

  override def onStop() : Unit = {}

  override def receiveUnManagedMessage: Receive = {
    case Error(ex) =>
      LOG.error("the stream has error", ex)
    case AkkaStreamMessage(msg) =>
      LOG.error("we have received message from akka stream source: " + msg)
      taskContext.output(Message(msg, System.currentTimeMillis()))
    case Complete(description) =>
      LOG.error("the stream is completed: " + description)
    case msg =>
      LOG.error("Failed! Received unknown message " + "taskId: " + taskId + ", " + msg.toString)
  }
}


object SourceBridgeTask {
  case class Error(ex: java.lang.Throwable)

  case class Complete(description: String)

  case class AkkaStreamMessage(msg: AnyRef)

  class SourceBridgeTaskClient[T <: AnyRef](ec: ExecutionContext, context: ClientContext, appId: Int, processorId: ProcessorId) extends Subscriber[T] {
    val taskId = TaskId(processorId, 0)
    var subscription: Subscription = null
    implicit val dispatcher = ec

    val task = context.askAppMaster[TaskActorRef](appId, LookupTaskActorRef(taskId)).map{container =>
      // println("Successfully resolved taskRef for taskId " + taskId + ", " + container.task)
      container.task
    }

    override def onError(throwable: Throwable): Unit = {
      task.map(task => task ! Error(throwable))
    }

    override def onSubscribe(subscription: Subscription): Unit = {
      // when taskActorRef is resolved, request message from upstream
      this.subscription = subscription
      task.map(task => subscription.request(1))
    }

    override def onComplete(): Unit = {
      task.map(task => task ! Complete("the upstream is completed"))
    }

    override def onNext(t: T): Unit = {
      task.map{task =>
        task ! AkkaStreamMessage(t)
      }
      subscription.request(1)
    }
  }
}
