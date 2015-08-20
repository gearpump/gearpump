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

package io.gearpump.partitioner

import io.gearpump.Message
import scala.reflect.ClassTag
import org.apache.commons.lang.SerializationUtils

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

  def name: String

  def partitioner: Partitioner
}

class PartitionerObject(private [this] val _partitioner: Partitioner) extends PartitionerFactory with Serializable {

  override def name: String = partitioner.getClass.getName

  override def partitioner: Partitioner = SerializationUtils.clone(_partitioner).asInstanceOf[Partitioner]
}

class PartitionerByClassName(partitionerClass: String) extends PartitionerFactory with Serializable {
  override def name: String = partitionerClass

  override def partitioner: Partitioner = Class.forName(partitionerClass).newInstance().asInstanceOf[Partitioner]
}


/**
 * @param partitionerFactory
 */
case class PartitionerDescription(partitionerFactory: PartitionerFactory)


object Partitioner {
  val UNKNOWN_PARTITION_ID = -1

  def apply[T <: Partitioner](implicit clazz: ClassTag[T]): PartitionerDescription = {
    PartitionerDescription(new PartitionerByClassName(clazz.runtimeClass.getName))
  }
}