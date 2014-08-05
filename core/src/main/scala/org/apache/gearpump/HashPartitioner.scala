package org.apache.gearpump

import java.util.Random

/**
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

class HashPartitioner extends Partitioner {
  override def getPartition(msg : String, partitionNum : Int) : Int = {
    (msg.hashCode & Integer.MAX_VALUE) % partitionNum
  }
}

class ShufflePartitioner extends Partitioner {
  private var mySeed = 0
  private var count = 0


  override def getPartition(msg : String, partitionNum : Int) : Int = {

    if (mySeed == 0) {
      mySeed = newSeed
    }

    val result = ((count + mySeed) & Integer.MAX_VALUE) % partitionNum
    count = count + 1
    result
  }

  def newSeed = new Random().nextInt()
}