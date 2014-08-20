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
  trait Config {
    var port: Int = -1
  }

  def usage: List[String]

  def uuid: String

  def start(): Unit

  def commandHelp() = {
    Console.println("Usage:")
    usage.foreach(Console.println(_))
  }

  def validate(config: Config): Unit = {
    if(config.port == -1) {
      commandHelp()
      System.exit(-1)
    }
  }

  def parse(args: List[String], config: Config) :Unit = {
    def doParse(argument : List[String]) : Unit = {
      argument match {
        case Nil => Unit // true if everything processed successfully
        case "-port" :: port :: rest => 
          config.port = port.toInt
          doParse(rest)
        case _ :: rest =>
          doParse(rest)
      }
    }
    doParse(args)
  }

}


