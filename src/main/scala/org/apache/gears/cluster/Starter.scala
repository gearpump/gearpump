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

package org.apache.gears.cluster

/**
 * Command line tool to start master, worker, client, and local mode
 */
trait Starter {
  case class Config(local : Boolean = false, port : Int = -1,
                    sameProcess : Boolean = false, role : String = "",
                    split: Int = 1, sum : Int = 1, runseconds : Int = 60,
                    workerCount : Int = 1, ip : String = "", arguments : Array[String] = null)

  def usage: List[String]

  def uuid: String

  def start(): Unit

  def commandHelp() = {
    Console.println("Usage:")
    usage.foreach(Console.println(_))
  }

  def validate(): Unit

  def parse(args: List[String]) :Config = {
    var config = Config()

    def doParse(argument : List[String]) : Unit = {
      argument match {
        case Nil => Unit // true if everything processed successfully
        case "-port" :: port :: rest => 
          config = config.copy(port = port.toInt)
          doParse(rest)
        case "-sameprocess" :: sameprocess :: rest  =>
          config = config.copy(sameProcess = sameprocess.toBoolean)
          doParse(rest)
        case "-workernum" :: worker :: rest =>
          config = config.copy(workerCount = worker.toInt)
          doParse(rest)
        case "-ip" :: ip :: rest =>
          config = config.copy(ip = ip)
          doParse(rest)
        case _ :: rest =>
          doParse(rest)
      }
    }
    doParse(args)
    config
  }

}


