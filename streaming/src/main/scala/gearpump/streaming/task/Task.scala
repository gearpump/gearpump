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

package gearpump.streaming.task

import akka.actor.Actor.Receive
import akka.actor.{Cancellable, Props, ActorRef, ActorSystem}
import gearpump.Message
import gearpump.streaming.{TaskIndex, ProcessorId}
import gearpump.TimeStamp
import gearpump.cluster.UserConfig
import gearpump.transport.HostPort
import gearpump.util.LogUtil
import org.slf4j.Logger

import scala.concurrent.duration.FiniteDuration

case class TaskId(processorId : ProcessorId, index : TaskIndex)

object TaskId {
  def toLong(id : TaskId) = (id.processorId.toLong << 32) + id.index
  def fromLong(id : Long) = TaskId(((id >> 32) & 0xFFFFFFFF).toInt, (id & 0xFFFFFFFF).toInt)
}

case class TaskLocations(locations : Map[HostPort, Set[TaskId]])
case object CleanTaskLocations


/**
 * This provides context information for a task.
 */
trait TaskContext {

  def taskId: TaskId

  def executorId: Int

  def appId : Int

  def appName: String

  /**
   * The actorRef of AppMaster
   * @return
   */
  def appMaster : ActorRef

  /**
   * The task parallelism
   *
   * For example, we can create 3 source tasks, and 3 sink tasks,
   * the task parallelism is 3 for each.
   *
   * This can be useful when reading from partitioned data source.
   * For example, for kafka, there may be 10 partitions, if we have
   * parallelism of 2 for this task, then each task will be responsible
   * to read data from 5 partitions.
   *
   * @return
   */
  def parallelism: Int


  /**
   * Please don't use this if possible.
   * @return
   */
  //TODO: We should remove the self from TaskContext
  def self: ActorRef

  /**
   * Please don't use this if possible
   * @return
   */
  //TODO: we should remove this in future
  def system: ActorSystem

  /**
   * This can be used to output messages to downstream tasks.
   *
   * The data shuffling rule can be decided by Partitioner.
   *
   * @param msg
   */
  def output(msg : Message) : Unit


  /**
   * @see ActorRefProvider.actorOf
   */
  def actorOf(props: Props): ActorRef

  /**
   * @see ActorRefProvider.actorOf
   */
  def actorOf(props: Props, name: String): ActorRef

  /**
   * @see ActorRefProducer.schedule
   */
  def schedule(initialDelay: FiniteDuration, interval: FiniteDuration)(f: ⇒ Unit): Cancellable

  /**
   *
   * ActorRefProvider.scheduleOnce
   * @param initialDelay
   * @param f
   * @return
   */
  def scheduleOnce(initialDelay: FiniteDuration)(f: ⇒ Unit): Cancellable

   /**
   * For managed message(type of Message), the sender only serve as a unique Id,
   * It's address is not something meaningful, you should not use this directly
   *
   * For unmanaged message, the sender represent the sender ActorRef
   * @return
   */
   def sender: ActorRef


  /**
   * retrieve upstream min clock from TaskActor
   * @return
   */
  def upstreamMinClock: TimeStamp
}

/**
 * Streaming Task interface
 */
trait TaskInterface {

  /**
   * @param startTime startTime that can be used to decide from when a source producer task should replay the data source, or from when a processor task should recover its checkpoint data in to in-memory state.
   */
  def onStart(startTime : StartTime) : Unit

  /**
   * @param msg message send by upstream tasks
   */
  def onNext(msg : Message) : Unit

  /**
   * This can be used to cleanup resource when the application finished.
   */
  def onStop() : Unit

  /**
   * handler for unmanaged message
  * @return
  */
  def receiveUnManagedMessage: Receive = null

  /**
   * This represent the min-clock of state
   * None means there is no in-memory state, Or the in-memory state doesn't
   * requires recovery.
   * @return
   */
  def stateClock: Option[TimeStamp]
}

abstract class Task(taskContext : TaskContext, userConf : UserConfig) extends TaskInterface{

  import taskContext.{appId, executorId, taskId}

  val LOG: Logger = LogUtil.getLogger(getClass, app = appId, executor = executorId, task = taskId)

  protected implicit val system = taskContext.system

  implicit val self = taskContext.self

  /**
   * For managed message(type of Message), the sender mean nothing,
   * you should not use this directory
   *
   * For unmanaged message, the sender represent the sender actor
   * @return
   */
  protected def sender: ActorRef = taskContext.sender

  def onStart(startTime : StartTime) : Unit = {}

  def onNext(msg : Message) : Unit = {}

  def onStop() : Unit = {}

  override def receiveUnManagedMessage: Receive = {
    case msg =>
      LOG.error("Failed! Received unknown message " + "taskId: " + taskId + ", " + msg.toString)
  }

  /**
   * This represent the min-clock of state
   * None means there is no in-memory state, Or the in-memory state doesn't
   * requires recovery.
   * @return
   */
  override def stateClock: Option[TimeStamp] = None
}