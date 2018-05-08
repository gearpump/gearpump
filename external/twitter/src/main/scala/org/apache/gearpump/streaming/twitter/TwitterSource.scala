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
package org.apache.gearpump.streaming.twitter

import java.time.Instant
import java.util.concurrent.LinkedBlockingQueue

import org.apache.gearpump.Message
import org.apache.gearpump.streaming.source.DataSource
import org.apache.gearpump.streaming.task.TaskContext
import org.apache.gearpump.streaming.twitter.TwitterSource.{Factory, MessageListener}
import twitter4j._
import twitter4j.conf.Configuration

class TwitterSource private[twitter](
    twitterFactory: Factory,
    filterQuery: Option[FilterQuery],
    statusListener: MessageListener
) extends DataSource {

  private var twitterStream: TwitterStream = _

  /**
   * Opens connection to data source
   * invoked in onStart() method of [[org.apache.gearpump.streaming.source.DataSourceTask]]
   *
   * @param context   is the task context at runtime
   * @param startTime is the start time of system
   */
  override def open(context: TaskContext, startTime: Instant): Unit = {

    this.twitterStream = twitterFactory.getTwitterStream
    this.twitterStream.addListener(statusListener)

    filterQuery match {
      case Some(query) =>
        this.twitterStream.filter(query)
      case None =>
        this.twitterStream.sample()
    }
  }

  /**
   * Reads next message from data source and
   * returns null if no message is available
   *
   * @return a [[org.apache.gearpump.Message]] or null
   */
  override def read(): Message = {
    Option(statusListener.poll()).map(status =>
      Message(status.getText, Instant.now())).orNull
  }

  /**
   * Closes connection to data source.
   * invoked in onStop() method of [[org.apache.gearpump.streaming.source.DataSourceTask]]
   */
  override def close(): Unit = {
    if (twitterStream != null) {
      twitterStream.shutdown()
    }
  }

  /**
   * Returns a watermark such that no timestamp earlier than the watermark should enter the system
   * Watermark.MAX mark the end of source data
   */
  override def getWatermark: Instant = {
    Instant.now()
  }
}

object TwitterSource {

  class MessageListener extends StatusListener with Serializable {

    private val queue = new LinkedBlockingQueue[Status](100000)

    def poll(): Status = {
      queue.poll()
    }

    override def onStallWarning(warning: StallWarning): Unit = {}

    override def onDeletionNotice(statusDeletionNotice: StatusDeletionNotice): Unit = {}

    override def onScrubGeo(userId: Long, upToStatusId: Long): Unit = {}

    override def onStatus(status: Status): Unit = {
      queue.offer(status)
    }

    override def onTrackLimitationNotice(numberOfLimitedStatuses: Int): Unit = {}

    override def onException(ex: Exception): Unit = {
      throw ex
    }
  }

  /**
   * Wrapper around TwitterStreamFactory which is final class and
   * can not be mocked
   */
  class Factory(factory: TwitterStreamFactory) extends Serializable {

    def getTwitterStream: TwitterStream = {
      factory.getInstance()
    }
  }

  def apply(conf: Configuration): TwitterSource = {
    new TwitterSource(new Factory(new TwitterStreamFactory(conf)),
      None, new MessageListener)
  }

  def apply(conf: Configuration, query: FilterQuery): TwitterSource = {
    new TwitterSource(new Factory(new TwitterStreamFactory(conf)),
      Option(query), new MessageListener)
  }
}
