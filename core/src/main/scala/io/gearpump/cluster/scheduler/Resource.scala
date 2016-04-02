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
package io.gearpump.cluster.scheduler

import akka.actor.ActorRef
import io.gearpump.WorkerId

case class Resource(slots : Int) {
  def +(other : Resource): Resource = Resource(slots + other.slots)

  def -(other : Resource): Resource = Resource(slots - other.slots)

  def >(other : Resource): Boolean = slots > other.slots

  def >=(other : Resource): Boolean = !(this < other)

  def <(other : Resource): Boolean = slots < other.slots

  def <=(other : Resource): Boolean = !(this > other)

  def equals(other : Resource): Boolean = slots == other.slots

  def isEmpty: Boolean = slots == 0
}

object Priority extends Enumeration{
  type Priority = Value
  val LOW, NORMAL, HIGH = Value
}

object Relaxation extends Enumeration{
  type Relaxation = Value

  // Option ONEWORKER allow user to schedule a task on specific worker.
  val ANY, ONEWORKER, SPECIFICWORKER = Value
}

import Relaxation._
import Priority._
case class ResourceRequest(resource: Resource,  workerId: WorkerId, priority: Priority = NORMAL, relaxation: Relaxation = ANY, executorNum: Int = 1)

case class ResourceAllocation(resource : Resource, worker : ActorRef, workerId : WorkerId)

object Resource {
  def empty = new Resource(0)

  def min(res1: Resource, res2: Resource) = if (res1.slots < res2.slots) res1 else res2
}

