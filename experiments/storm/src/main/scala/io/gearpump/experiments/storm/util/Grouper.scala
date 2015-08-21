/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.gearpump.experiments.storm.util

import java.util.{List => JList}

import backtype.storm.tuple.Fields
import clojure.lang.RT

import scala.util.Random

sealed trait Grouper {
  def getPartition(taskId: Int, values: JList[AnyRef]): Int
}

class GlobalGrouper extends Grouper {
  override def getPartition(taskId: Int, values: JList[AnyRef]): Int = 0
}

class NoneGrouper(numTasks: Int) extends Grouper {
  val random = new Random
  lazy val mod = RT.`var`("clojure.core", "mod")

  override def getPartition(taskId: Int, values: JList[AnyRef]): Int = {
    mod.invoke(random.nextInt, numTasks).asInstanceOf[java.lang.Long].toInt
  }
}

class ShuffleGrouper(numTasks: Int) extends Grouper {
  private val random = new Random
  private var index = -1
  private var partitions = List.empty[Int]

  override def getPartition(taskId: Int, values: JList[AnyRef]): Int = {
    index += 1
    if (partitions.isEmpty) {
      partitions = 0.until(numTasks).toList
      partitions = random.shuffle(partitions)
    } else if (index >= numTasks) {
      index = 0
      partitions = random.shuffle(partitions)
    }
    partitions(index)
  }
}

class FieldsGrouper(outFields: Fields, groupFields: Fields, numTasks: Int) extends Grouper {

  override def getPartition(taskId: Int, values: JList[AnyRef]): Int = {
    val hash = outFields.select(groupFields, values).hashCode()
    (hash & Integer.MAX_VALUE) % numTasks
  }
}

