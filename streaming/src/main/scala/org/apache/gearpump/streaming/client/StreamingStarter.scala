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
package org.apache.gearpump.streaming.client

import java.net.URLClassLoader

import org.apache.gearpump.cluster.main.{AppSubmitter, Starter, ParseResult, ArgumentsParser}
import org.apache.gearpump.streaming.AppDescription
import org.slf4j.{LoggerFactory, Logger}

trait StreamingStarter extends Starter {
  this: ArgumentsParser =>
  private val LOG: Logger = LoggerFactory.getLogger(classOf[StreamingStarter])

  def main(args: Array[String]): Unit = {
    val config = parse(args)
    val masters = config.getString("master")
    val runseconds = config.getInt("runseconds")
    LOG.info("Master URL: " + masters)
    val context = ClientContext(masters)
    val app = application(config).asInstanceOf[AppDescription]
    val appId = context.submit(app, app.conf, Option(AppSubmitter.jars(0)))
    LOG.info(s"We get application id: $appId")
    Thread.sleep(runseconds * 1000)
    LOG.info(s"Shutting down application $appId")
    context.shutdown(appId)
    context.destroy()
  }

}
