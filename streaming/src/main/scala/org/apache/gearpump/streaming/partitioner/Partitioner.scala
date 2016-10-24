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

package org.apache.gearpump.streaming.partitioner

import org.apache.commons.lang.SerializationUtils
import org.apache.gearpump.Message

import scala.reflect.ClassTag

/**
 * For processor chain: A (3 tasks) {@literal ->} B (3 tasks), partitioner decide how ONE task
 * of upstream processor A send to several tasks of downstream processor B.
 */
sealed trait Partitioner extends Serializable

/**
 * For processor chain: A (3 tasks) {@literal ->} B (3 tasks), UnicastPartitioner does
 * ONE-task {@literal ->} ONE-task mapping.
 */
trait UnicastPartitioner extends Partitioner {

  /**
   * Gets the SINGLE downstream processor task index to send message to.
   *
   * @param msg Message you want to send
   * @param partitionNum How many tasks does the downstream processor have.
   * @param upstreamTaskIndex Upstream task's task index who trigger the getPartition() call.
   *
   * @return ONE task index of downstream processor.
   */
  def getPartition(msg: Message, partitionNum: Int, upstreamTaskIndex: Int): Int

  def getPartition(msg: Message, partitionNum: Int): Int = {
    getPartition(msg, partitionNum, Partitioner.UNKNOWN_PARTITION_ID)
  }
}

trait MulticastPartitioner extends Partitioner {

  /**
   * Gets a list of downstream processor task indexes to send message to.
   *
   * @param upstreamTaskIndex Current sender task's task index.
   *
   */
  def getPartitions(msg: Message, partitionNum: Int, upstreamTaskIndex: Int): Array[Int]

  def getPartitions(msg: Message, partitionNum: Int): Array[Int] = {
    getPartitions(msg, partitionNum, Partitioner.UNKNOWN_PARTITION_ID)
  }
}

sealed trait PartitionerFactory {

  def name: String

  def partitioner: Partitioner
}

/** Stores the Partitioner in an object. To use it, user need to deserialize the object */
class PartitionerObject(private[this] val _partitioner: Partitioner)
  extends PartitionerFactory with Serializable {

  override def name: String = partitioner.getClass.getName

  override def partitioner: Partitioner = {
    SerializationUtils.clone(_partitioner).asInstanceOf[Partitioner]
  }
}

/** Store the partitioner in class Name, the user need to instantiate a new class */
class PartitionerByClassName(partitionerClass: String)
  extends PartitionerFactory with Serializable {

  override def name: String = partitionerClass
  override def partitioner: Partitioner = {
    Class.forName(partitionerClass).newInstance().asInstanceOf[Partitioner]
  }
}

/**
 * @param partitionerFactory How we construct a Partitioner.
 */
case class PartitionerDescription(partitionerFactory: PartitionerFactory)

object Partitioner {
  val UNKNOWN_PARTITION_ID = -1

  def apply[T <: Partitioner](implicit clazz: ClassTag[T]): PartitionerDescription = {
    PartitionerDescription(new PartitionerByClassName(clazz.runtimeClass.getName))
  }
}