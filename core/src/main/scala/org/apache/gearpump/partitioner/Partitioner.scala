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

package org.apache.gearpump.partitioner

import org.apache.gearpump.Message
import org.apache.gearpump.cluster.UserConfig

import scala.reflect.ClassTag

trait Partitioner extends Serializable {

  /**
   *
   * @param msg
   * @param partitionNum
   * @param currentPartitionId, used when the downstream processor want to share the same
   *   partition id,
   * @return
   */
  def getPartition(msg : Message, partitionNum : Int, currentPartitionId: Int) : Int

  def getPartition(msg : Message, partitionNum : Int) : Int = {
    getPartition(msg, partitionNum, Partitioner.UNKNOWN_PARTITION_ID)
  }
}

sealed trait PartitionerFactory {
  def partitioner: Partitioner
}

case class PartitionerObject(partitioner: Partitioner) extends PartitionerFactory

case class PartitionerByClassName(partitionerClass: String) extends PartitionerFactory {
  def partitioner: Partitioner = Class.forName(partitionerClass).newInstance().asInstanceOf[Partitioner]
}

case class LifeTime(birth: Long, die: Long)

object LifeTime {
  val Immortal = LifeTime(0L, Long.MaxValue)
}

/**
 * @param partitionerFactory
 */
case class PartitionerDescription(partitionerFactory: PartitionerFactory, life: LifeTime = LifeTime.Immortal)


object Partitioner {
  val UNKNOWN_PARTITION_ID = -1

  def apply[T <: Partitioner](implicit clazz: ClassTag[T]): PartitionerDescription = {
    PartitionerDescription(PartitionerByClassName(clazz.runtimeClass.getName))
  }
}

