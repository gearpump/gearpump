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

package org.apache.gearpump

import akka.actor.Actor
import org.apache.gearpump.util.{Graph, ReferenceEqual}
import org.apache.gears.cluster.Configs

case class TaskDescription(taskClass: Class[_ <: Actor], parallism : Int) extends ReferenceEqual
//case class StageDescription(task : TaskDescription, parallism : Int)

case class AppDescription(name : String, conf : Configs, dag: Graph[TaskDescription, Partitioner]) extends org.apache.gears.cluster.Application with ReferenceEqual

