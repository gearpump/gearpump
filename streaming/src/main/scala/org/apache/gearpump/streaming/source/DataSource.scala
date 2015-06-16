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

package org.apache.gearpump.streaming.source

import org.apache.gearpump.Message

/**
 * interface to implement custom source
 * from which data is read into the system
 *
 * an example would be like
 * {{{
 *  GenStringSource extends DataSource {
 *    def read(num: Int): List[Message] = {
 *      val messages =
 *        for { i <- 0 until num
 *        } yield Message(s"message-$i", System.currentTimeMillis())
 *      messages.toList
 *    }
 *
 *    def close(): Unit = {}
 *  }
 * }}}
 */
trait DataSource {

  /**
   *  read 'num' messages from source
   *  Note: this is best effort. The returned
   *  message count may be less than 'num'
   *
   *  @param num number of messages to read
   */
  def read(num: Int): List[Message]

  def close(): Unit
}
