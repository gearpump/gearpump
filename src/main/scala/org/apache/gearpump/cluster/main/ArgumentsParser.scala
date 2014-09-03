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

package org.apache.gearpump.cluster.main

class ParseResult(optionMap : Map[String, String], remainArguments : Array[String]) {
  def getInt(key : String) = optionMap.get(key).get.toInt

  def getString (key : String) = optionMap.get(key).get

  def exists(key : String) = optionMap.get(key).isDefined

  def remainArgs = this.remainArguments
}

trait ArgumentsParser {

  def help = {
    Console.println("Usage:")
    val usage = List(s"java ${this.getClass} " + options.map(kv => s"-${kv._1} <${kv._2}>").mkString(" ") + " " + remainArgs.map(k => s"<$k>").mkString(" "))
    usage.foreach(Console.println(_))
  }

  val options : Array[(String, String)]
  val remainArgs : Array[String] = Array.empty[String]

  def parse(args: Array[String]) : ParseResult = {

    var config = Map.empty[String, String]
    var remainArgs = Array.empty[String]

    def doParse(argument : List[String]) : Unit = {
      argument match {
        case Nil => Unit // true if everything processed successfully

        case key :: value :: rest if (key.startsWith("-") && !value.startsWith("-")) =>
          val fixedKey = key.substring(1)
          config += fixedKey -> value
          if (!options.map(_._1).contains(fixedKey)) {
            help
            throw new Exception(s"found unknown option $fixedKey")
          }
          doParse(rest)

        case key :: rest if key.startsWith("-") =>
          val fixedKey = key.substring(1)
          config += fixedKey -> ""
          if (!options.map(_._1).contains(fixedKey)) {
            help
            throw new Exception(s"found unknown option $fixedKey")
          }
          doParse(rest)

        case value :: rest =>
          remainArgs :+= value
          doParse(rest)
      }
    }
    doParse(args.toList)

    if (remainArgs.length != this.remainArgs.length ||
    options.length != config.keySet.size) {
      help
      Console.println(s"fail to parse arguments...")
      System.exit(-1)
    }

    new ParseResult(config, args)
  }
}