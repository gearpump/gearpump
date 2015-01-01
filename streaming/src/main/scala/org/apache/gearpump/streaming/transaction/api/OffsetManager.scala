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

package org.apache.gearpump.streaming.transaction.api

import org.apache.gearpump.{Message, TimeStamp}

import scala.util.Try

/**
 * filter offsets and store the mapping from timestamp to offset
 */
trait MessageFilter {
  def filter(messageAndOffset: (Message, Long)): Option[Message]
}

/**
 * resolve timestamp to offset by look up the underlying storage
 */
trait OffsetTimeStampResolver {
  def resolveOffset(time: TimeStamp): Try[Long]
}

/**
 * manages message's offset on TimeReplayableSource and timestamp
 */
trait OffsetManager extends MessageFilter with OffsetTimeStampResolver

