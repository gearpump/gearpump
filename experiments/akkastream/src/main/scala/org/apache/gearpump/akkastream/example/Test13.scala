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

package org.apache.gearpump.akkastream.example

import java.time._

import akka.actor.ActorSystem
import akka.stream.scaladsl.Source
import org.apache.gearpump.akkastream.GearpumpMaterializer
import org.apache.gearpump.akkastream.scaladsl.Implicits._
import org.apache.gearpump.cluster.main.ArgumentsParser
import org.apache.gearpump.util.AkkaApp

import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Random

/**
 * GroupBy example
 */

/*
// Original example
val f = Source
  .tick(0.seconds, 1.second, "")
  .map { _ =>
    val now = System.currentTimeMillis()
    val delay = random.nextInt(8)
    MyEvent(now - delay * 1000L)
  }
  .statefulMapConcat { () =>
    val generator = new CommandGenerator()
    ev => generator.forEvent(ev)
  }
  .groupBy(64, command => command.w)
  .takeWhile(!_.isInstanceOf[CloseWindow])
  .fold(AggregateEventData((0L, 0L), 0))({
    case (agg, OpenWindow(window)) => agg.copy(w = window)
    // always filtered out by takeWhile
    case (agg, CloseWindow(_)) => agg
    case (agg, AddToWindow(ev, _)) => agg.copy(eventCount = agg.eventCount + 1)
  })
  .async
  .mergeSubstreams
  .runForeach { agg =>
    println(agg.toString)
  }
 */
object Test13 extends AkkaApp with ArgumentsParser {
  // scalastyle:off println

  override def main(akkaConf: Config, args: Array[String]): Unit = {

    implicit val system = ActorSystem("Test13", akkaConfig)
    implicit val materializer = GearpumpMaterializer()

    val random = new Random()

    val result = Source
      .tick(0.seconds, 1.second, "tick data")
      .map { _ =>
        val now = System.currentTimeMillis()
        val delay = random.nextInt(8)
        MyEvent(now - delay * 1000L)
      }
      .statefulMapConcat { () =>
        val generator = new CommandGenerator()
        ev => generator.forEvent(ev)
      }
      .groupBy2(command => command.w)
      .takeWhile(!_.isInstanceOf[CloseWindow])
      .fold(AggregateEventData((0L, 0L), 0))({
        case (agg, OpenWindow(window)) => agg.copy(w = window)
        // always filtered out by takeWhile
        case (agg, CloseWindow(_)) => agg
        case (agg, AddToWindow(ev, _)) => agg.copy(eventCount = agg.eventCount + 1)
      })
      .runForeach(agg =>
        println(agg.toString)
      )

    Await.result(system.whenTerminated, 60.minutes)
  }

  case class MyEvent(timestamp: Long)

  type Window = (Long, Long)

  object Window {
    val WindowLength = 10.seconds.toMillis
    val WindowStep = 1.second.toMillis
    val WindowsPerEvent = (WindowLength / WindowStep).toInt

    def windowsFor(ts: Long): Set[Window] = {
      val firstWindowStart = ts - ts % WindowStep - WindowLength + WindowStep
      (for (i <- 0 until WindowsPerEvent) yield
        (firstWindowStart + i * WindowStep,
          firstWindowStart + i * WindowStep + WindowLength)
        ).toSet
    }
  }

  sealed trait WindowCommand {
    def w: Window
  }

  case class OpenWindow(w: Window) extends WindowCommand

  case class CloseWindow(w: Window) extends WindowCommand

  case class AddToWindow(ev: MyEvent, w: Window) extends WindowCommand

  class CommandGenerator {
    private val MaxDelay = 5.seconds.toMillis
    private var watermark = 0L
    private val openWindows = mutable.Set[Window]()

    def forEvent(ev: MyEvent): List[WindowCommand] = {
      watermark = math.max(watermark, ev.timestamp - MaxDelay)
      if (ev.timestamp < watermark) {
        println(s"Dropping event with timestamp: ${tsToString(ev.timestamp)}")
        Nil
      } else {
        val eventWindows = Window.windowsFor(ev.timestamp)

        val closeCommands = openWindows.flatMap { ow =>
          if (!eventWindows.contains(ow) && ow._2 < watermark) {
            openWindows.remove(ow)
            Some(CloseWindow(ow))
          } else None
        }

        val openCommands = eventWindows.flatMap { w =>
          if (!openWindows.contains(w)) {
            openWindows.add(w)
            Some(OpenWindow(w))
          } else None
        }

        val addCommands = eventWindows.map(w => AddToWindow(ev, w))

        openCommands.toList ++ closeCommands.toList ++ addCommands.toList
      }
    }
  }

  case class AggregateEventData(w: Window, eventCount: Int) {
    override def toString: String =
      s"Between ${tsToString(w._1)} and ${tsToString(w._2)}, there were $eventCount events."
  }

  def tsToString(ts: Long): String = OffsetDateTime
    .ofInstant(Instant.ofEpochMilli(ts), ZoneId.systemDefault())
    .toLocalTime
    .toString
  // scalastyle:on println

}


