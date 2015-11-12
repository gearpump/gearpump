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

import java.util
import java.util.concurrent.TimeUnit

import akka.actor.Actor.Receive
import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.stream.gearpump.task.SinkBridgeTask.RequestMessage
import akka.util.Timeout
import io.gearpump.Message
import io.gearpump.cluster.UserConfig
import io.gearpump.cluster.client.ClientContext
import io.gearpump.streaming.ProcessorId
import io.gearpump.streaming.appmaster.AppMaster.{LookupTaskActorRef, TaskActorRef}
import io.gearpump.streaming.task.{StartTime, Task, TaskContext, TaskId}
import io.gearpump.util.LogUtil
import org.reactivestreams.{Publisher, Subscriber, Subscription}

import scala.concurrent.ExecutionContext

/**
 * Bridge Task when data flow is from remote Gearpump Task to local Akka-Stream Module
 *
 *
 * upstream [[Task]] -> [[SinkBridgeTask]]
 *                         \              Remote Cluster
 * -------------------------\----------------------
 *                           \            Local JVM
 *                            \|
 *                       Akka Stream [[Subscriber]]
 *
 *
 * @param taskContext
 * @param userConf
 */
class SinkBridgeTask (taskContext : TaskContext, userConf : UserConfig) extends Task(taskContext, userConf) {
  import taskContext.taskId

  val queue = new util.LinkedList[Message]()
  var subscriber: ActorRef = null

  var request: Int = 0

  override def onStart(startTime : StartTime) : Unit = {}

  override def onNext(msg : Message) : Unit = {
    queue.add(msg)
    trySendingData()
  }

  override def onStop() : Unit = {}

  private def trySendingData(): Unit = {
    if (subscriber != null) {
      (0 to request).map(_ => queue.poll()).filter(_ != null).foreach { msg =>
        subscriber ! msg.msg
        request -= 1
      }
    }
  }

  override def receiveUnManagedMessage: Receive = {
    case RequestMessage(n) =>
      this.subscriber = sender
      LOG.info("the downstream has requested " + n + " messages from " + subscriber)
      request += n.toInt
      trySendingData()
    case msg =>
      LOG.error("Failed! Received unknown message " + "taskId: " + taskId + ", " + msg.toString)
  }
}

object SinkBridgeTask {

  case class RequestMessage(number: Int)

  class SinkBridgeTaskClient(system: ActorSystem, context: ClientContext, appId: Int, processorId: ProcessorId) extends Publisher[AnyRef] with Subscription {
    private val taskId = TaskId(processorId, index = 0)

    private val LOG = LogUtil.getLogger(getClass)

    private var actor: ActorRef = null
    import system.dispatcher

    private val task = context.askAppMaster[TaskActorRef](appId, LookupTaskActorRef(taskId)).map{container =>
      // println("Successfully resolved taskRef for taskId " + taskId + ", " + container.task)
      container.task
    }

    override def subscribe(subscriber: Subscriber[_ >: AnyRef]): Unit = {
      this.actor = system.actorOf(Props(new ClientActor(subscriber)))
      subscriber.onSubscribe(this)
    }

    override def cancel(): Unit = Unit

    private implicit val timeout = Timeout(5, TimeUnit.SECONDS)

    override def request(l: Long): Unit = {
      task.foreach{ task =>
        task.tell(RequestMessage(l.toInt), actor)
      }
    }
  }

  class ClientActor(subscriber: Subscriber[_ >: AnyRef]) extends Actor {
    def receive: Receive = {
      case result: AnyRef => subscriber.onNext(result)
    }
  }
}