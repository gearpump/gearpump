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

package io.gearpump.streaming.task

import akka.actor.Actor._
import akka.actor.{ActorRef, ActorSystem, Cancellable, Props}
import io.gearpump.{TimeStamp, Message}
import io.gearpump.cluster.UserConfig
import io.gearpump.util.LogUtil
import org.slf4j.Logger

import scala.concurrent.duration.FiniteDuration

/**
 * This provides TaskContext for user defined tasks
 * @param taskClass task class
 * @param context context class
 * @param userConf user config
 */
class TaskWrapper(val taskId: TaskId, taskClass: Class[_ <: Task], context: TaskContextData, userConf: UserConfig) extends TaskContext with TaskInterface {

  private val LOG = LogUtil.getLogger(taskClass, task = taskId)

  private var actor: TaskActor = null

  private var task: Option[Task] = None

  def setTaskActor(actor: TaskActor): Unit = this.actor = actor

  override def appId: Int = context.appId

  override def appName: String = context.appName

  override def executorId: Int = context.executorId

  override def parallelism: Int = context.parallelism

  override def appMaster: ActorRef = context.appMaster

  override def output(msg: Message): Unit = actor.output(msg)

  /**
   * @see [[TaskActor.output]]
   * @param index, not same as ProcessorId
   * @param msg
   */
  def output(index: Int, msg: Message): Unit = actor.output(index, msg)

  /**
   * Use with caution, output unmanaged message to target tasks
   * @param msg
   * @param tasks
   */
  def outputUnManaged(msg: AnyRef, tasks: TaskId *): Unit = {
    actor.transport(msg, tasks: _*)
  }

  override def self: ActorRef = actor.context.self

  override def sender: ActorRef = actor.context.sender()

  def system: ActorSystem = actor.context.system

  /**
   * @see ActorRefProvider.actorOf
   */
  override def actorOf(props: Props): ActorRef = actor.context.actorOf(props)

  /**
   * @see ActorRefProvider.actorOf
   */
  override def actorOf(props: Props, name: String): ActorRef = actor.context.actorOf(props, name)

  override def onStart(startTime: StartTime): Unit = {
    if (None != task) {
      LOG.error(s"Task.onStart should not be called multiple times... ${task.getClass}")
    }
    val constructor = taskClass.getConstructor(classOf[TaskContext], classOf[UserConfig])
    task = Some(constructor.newInstance(this, userConf))
    task.foreach(_.onStart(startTime))
  }

  override def onNext(msg: Message): Unit = task.foreach(_.onNext(msg))

  override def onStop(): Unit = {
    task.foreach(_.onStop())
    task = None
  }

  override def receiveUnManagedMessage: Receive = {
    task.map(_.receiveUnManagedMessage).getOrElse(defaultMessageHandler)
  }

  override def upstreamMinClock: TimeStamp = {
    actor.getUpstreamMinClock
  }

  def schedule(initialDelay: FiniteDuration, interval: FiniteDuration)(f: ⇒ Unit): Cancellable = {
    val dispatcher = actor.context.system.dispatcher
    actor.context.system.scheduler.schedule(initialDelay, interval)(f)(dispatcher)
  }

  def scheduleOnce(initialDelay: FiniteDuration)(f: ⇒ Unit): Cancellable = {
    val dispatcher = actor.context.system.dispatcher
    actor.context.system.scheduler.scheduleOnce(initialDelay)(f)(dispatcher)
  }

  private def defaultMessageHandler: Receive = {
    case msg =>
      LOG.error("Failed! Received unknown message " + "taskId: " + taskId + ", " + msg.toString)
  }

  /**
   * logger is environment dependant, it should be provided by
   * containing environment.
   * @return
   */
  override def logger: Logger = LOG
}