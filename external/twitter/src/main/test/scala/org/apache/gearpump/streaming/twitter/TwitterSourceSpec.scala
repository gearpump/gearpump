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

import org.apache.gearpump.streaming.MockUtil
import org.apache.gearpump.streaming.twitter.TwitterSource.{Factory, MessageListener}
import org.mockito.Mockito._
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.mock.MockitoSugar
import org.scalatest.{Matchers, PropSpec}
import org.scalatest.prop.PropertyChecks
import twitter4j.{FilterQuery, TwitterStream}

class TwitterSourceSpec extends PropSpec with PropertyChecks with Matchers with MockitoSugar {

  implicit val arbQuery: Arbitrary[Option[FilterQuery]] = Arbitrary {
    Gen.oneOf(None, Some(new FilterQuery()))
  }

  property("TwitterSource should properly setup, poll message and teardown") {
    forAll {
      (query: Option[FilterQuery], startTime: Long) =>
        val factory = mock[Factory]
        val stream = mock[TwitterStream]
        val listener = mock[MessageListener]

        when(factory.getTwitterStream).thenReturn(stream)
        val twitterSource = new TwitterSource(factory, query, listener)

        twitterSource.open(MockUtil.mockTaskContext, Instant.ofEpochMilli(startTime))

        verify(stream).addListener(listener)
        query match {
          case Some(q) =>
            verify(stream).filter(q)
          case None =>
            verify(stream).sample()
        }

        twitterSource.read()
        verify(listener).poll()

        twitterSource.close()
        verify(stream).shutdown()
    }
  }
}
