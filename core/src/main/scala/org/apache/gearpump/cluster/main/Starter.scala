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

import org.apache.gearpump.cluster.client.ClientContext
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Success}

trait Starter {
  this: ArgumentsParser =>
  private val LOG: Logger = LoggerFactory.getLogger(classOf[Starter])

  def main(args: Array[String]): Unit = {
    val config = parse(args)
    val masters = config.getString("master")
    val runseconds = config.getInt("runseconds")
    LOG.info("Master URL: " + masters)
    val context = ClientContext(masters)
    val app = application(config)
    val appId = context.submit(app, Option(AppSubmitter.jars(0)))
    appId match {
      case Success(appId) =>
        LOG.info(s"We get application id: $appId")
        Thread.sleep(runseconds * 1000)
        LOG.info(s"Shutting down application $appId")
        context.shutdown(appId)
        context.destroy()
      case Failure(failure) =>
        LOG.error("Failed to launch application", failure)
        context.destroy()
    }
  }

  def application(config: ParseResult): org.apache.gearpump.cluster.Application

}
