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

package akka.stream.gearpump.example

import java.io.{File, FileInputStream}
import java.util.zip.GZIPInputStream

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.gearpump.graph.GraphCutter
import akka.stream.gearpump.{GearAttributes, GearpumpMaterializer}
import akka.stream.scaladsl._
import akka.stream.{ClosedShape, IOResult}
import akka.util.ByteString
import io.gearpump.cluster.main.{ArgumentsParser, CLIOption}
import io.gearpump.util.AkkaApp
import org.json4s.JsonAST.JString

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

/**
 * this example is ported from http://engineering.intenthq.com/2015/06/wikidata-akka-streams/
 * which showcases running Akka Streams DSL across JVMs on Gearpump
 *
 * Usage: output/target/pack/bin/gear app -jar experiments/akkastream/target/scala_2.11/akkastream-${VERSION}-SNAPSHOT-assembly.jar
 *            -input wikidata-${DATE}-all.json.gz -languages en,de
 *
 * (Note: Wikipedia data can be downloaded from https://dumps.wikimedia.org/wikidatawiki/entities/)
 *
 */
object WikipediaApp extends ArgumentsParser with AkkaApp {

  case class WikidataElement(id: String, sites: Map[String, String])

  override val options: Array[(String, CLIOption[Any])] = Array(
    "input" -> CLIOption[String]("<Wikidata JSON dump>", required = true),
    "languages" -> CLIOption[String]("<languages to take into account>", required = true)
  )

  override def main(akkaConf: Config, args: Array[String]): Unit = {
    val parsed = parse(args)
    val input = new File(parsed.getString("input"))
    val langs = parsed.getString("languages").split(",")

    implicit val system = ActorSystem("wikidata-poc", akkaConf)
    implicit val materializer = new GearpumpMaterializer(system, GraphCutter.TagAttributeStrategy, useLocalCluster = false)
    import system.dispatcher

    val elements = source(input).via(parseJson(langs))

    val g = RunnableGraph.fromGraph(
      GraphDSL.create(count) { implicit b =>
        sinkCount => {
          import GraphDSL.Implicits._
          val broadcast = b.add(Broadcast[WikidataElement](2))
          elements ~> broadcast ~> logEveryNSink(1000)
          broadcast ~> checkSameTitles(langs.toSet) ~> sinkCount
          ClosedShape
        }
      }
    )

    g.run().onComplete { x =>
      x match {
        case Success((t, f)) => printResults(t, f)
        case Failure(tr) => println("Something went wrong")
      }
      system.shutdown()
    }
    system.awaitTermination()
  }

  def source(file: File): Source[String, Future[IOResult]] = {
    val compressed = new GZIPInputStream(new FileInputStream(file), 65536)
    StreamConverters.fromInputStream(() => compressed)
      .via(Framing.delimiter(ByteString("\n"), Int.MaxValue))
      .map(x => x.decodeString("utf-8"))
  }

  def parseJson(langs: Seq[String])(implicit ec: ExecutionContext): Flow[String, WikidataElement, NotUsed] =
    Flow[String].mapAsyncUnordered(8)(line => Future(parseItem(langs, line))).collect({
      case Some(v) => v
    })

  def parseItem(langs: Seq[String], line: String): Option[WikidataElement] = {
    import org.json4s.jackson.JsonMethods
    Try(JsonMethods.parse(line)).toOption.flatMap { json =>
      json \ "id" match {
        case JString(itemId) =>

          val sites:Seq[(String,String)] = for {
            lang <- langs
            JString(title) <- json \ "sitelinks" \ s"${lang}wiki" \ "title"
          } yield lang -> title

          if (sites.isEmpty) None
          else Some(WikidataElement(id = itemId, sites = sites.toMap))

        case _ => None
      }
    }
  }

  def logEveryNSink[T](n: Int) = Sink.fold(0) { (x, y: T) =>
    if (x % n == 0)
      println(s"Processing element $x: $y")
    x + 1
  }

  def checkSameTitles(langs: Set[String]): Flow[WikidataElement, Boolean, NotUsed] = Flow[WikidataElement]
    .filter(_.sites.keySet == langs)
    .map { x =>
      val titles = x.sites.values
      titles.forall( _ == titles.head)
    }.withAttributes(GearAttributes.remote)

  def count: Sink[Boolean, Future[(Int, Int)]] = Sink.fold((0,0)) {
    case ((t, f), true) => (t+1, f)
    case ((t, f), false) => (t, f+1)
  }

  def printResults(t: Int, f: Int) = {
    val message = s"""
                     | Number of items with the same title: $t
        | Number of items with the different title: $f
        | Ratios: ${t.toDouble / (t + f)} / ${f.toDouble / (t + f)}
                  """.stripMargin
    println(message)
  }

}
