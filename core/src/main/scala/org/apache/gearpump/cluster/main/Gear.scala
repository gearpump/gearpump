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

import org.slf4j.{Logger, LoggerFactory}

object Gear extends App {

  private val LOG: Logger = LoggerFactory.getLogger(Gear.getClass)

  def usage(commandOption: Option[String]) = {
    commandOption match {
      case Some(command) =>
        command match {
          case "kill" =>
            Kill.main(Array.empty[String])
          case "shell" =>
            Shell.main(Array.empty[String])
          case "info" =>
            Info.main(Array.empty[String])
          case "replay" =>
            Replay.main(Array.empty[String])
          case "app" =>
            AppSubmitter.main(Array.empty[String])

        }
      case None =>
        Console.println("Usage: app|info|kill|shell|replay ...")

    }
    System.exit(-1)
  }

  def start = {
    args.length match {
      case 0 =>
        usage(None)
      case a if(a < 3) =>
        val command = args(0)
        usage(Option(command))
      case _ =>
    }

    val command = args(0)
    val commandArgs = args.drop(1)

    command match {
      case "kill" =>
        Kill.main(commandArgs)
      case "shell" =>
        Shell.main(commandArgs)
      case "info" =>
        Info.main(commandArgs)
      case "replay" =>
        Replay.main(commandArgs)
      case "app" =>
        AppSubmitter.main(commandArgs)
    }
  }

  start
}
