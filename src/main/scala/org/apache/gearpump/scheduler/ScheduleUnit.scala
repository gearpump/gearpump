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
package org.apache.gearpump.scheduler

import akka.actor.ActorRef

class Resource(val slots : Int) extends Serializable

case class Allocation(override val slots : Int, worker : ActorRef) extends Resource(slots)

object Resource{
  val RESOURCE_MINIMUM_UNIT = new Resource(1)

  def apply(slots : Int) = new Resource(slots)

  implicit def int2Resource(slots : Int) = apply(slots)

  implicit class ResourceHelper(resource : Resource){
    def plus(other : Resource) = Resource(resource.slots + other.slots)

    def -(other : Resource) = Resource(resource.slots - other.slots)

    def >(other : Resource) = resource.slots > other.slots

    def <(other : Resource) = resource.slots < other.slots

    def ==(other : Resource) = resource.slots == other.slots
  }
}

object Allocation{
  implicit def allc2Resource(allc : Allocation) = Resource(allc.slots)
}