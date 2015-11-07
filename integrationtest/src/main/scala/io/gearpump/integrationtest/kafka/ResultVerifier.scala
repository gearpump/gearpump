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
package io.gearpump.integrationtest.kafka

import scala.collection.mutable

trait ResultVerifier {
  def onNext(msg: String): Unit
}

class MessageLossDetector(totalNum: Int) extends ResultVerifier {
  private val bitSets = new mutable.BitSet(totalNum)

  override def onNext(msg: String): Unit = {
    val num = msg.toInt
    bitSets.add(num)
  }

  def allReceived: Boolean = {
    1.to(totalNum).forall(bitSets)
  }

}