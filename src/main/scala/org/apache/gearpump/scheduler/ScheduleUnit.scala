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

case class Resource(slots : Int)

case class ResourceRequest(resource: Resource, worker : ActorRef)

case class Allocation(resource : Resource, worker : ActorRef)

object Resource{
  val RESOURCE_MINIMUM_UNIT = new Resource(1)

  implicit def int2Resource(slots : Int) = apply(slots)

  implicit def alloc2Resource(allocation : Allocation) = allocation.resource

  implicit def req2Resource(request : ResourceRequest) = request.resource

  implicit class ResourceHelper(resource : Resource){
    def plus(other : Resource) = Resource(resource.slots + other.slots)

    def -(other : Resource) = Resource(resource.slots - other.slots)

    def >(other : Resource) = resource.slots > other.slots

    def <(other : Resource) = resource.slots < other.slots

    def ==(other : Resource) = resource.slots == other.slots
  }
}
