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

import java.net.{URL, URLClassLoader}
import java.util.jar.JarFile

import org.apache.gearpump.util.{Constants, LogUtil, Util}
import org.slf4j.Logger

import scala.util.Try

object AppSubmitter extends App with ArgumentsParser {
  val LOG: Logger = LogUtil.getLogger(getClass)

  override val ignoreUnknownArgument = true

  override val options: Array[(String, CLIOption[Any])] = Array(
    "namePrefix" -> CLIOption[String]("<application name prefix>", required = false, defaultValue = Some("")),
    "jar" -> CLIOption("<application>.jar", required = true))

  def start : Unit = {

    Try({
      val config = parse(args)
      if (null == config) {
        return
      }

      val jar = config.getString("jar")

      // Set jar path to be submitted to cluster
      System.setProperty(Constants.GEARPUMP_APP_JAR, jar)

      val namePrefix = config.getString("namePrefix")
      if (namePrefix.nonEmpty) {
        if (!Util.validApplicationName(namePrefix)) {
          throw new Exception(s"$namePrefix is not a valid prefix for an application name")
        }
        System.setProperty(Constants.GEARPUMP_APP_NAME_PREFIX, namePrefix)
      }

      val jarFile = new java.io.File(jar)

      //start main class
      Option(jarFile.exists()) match {
        case Some(true) =>
          val mainClass = new JarFile(jarFile).getManifest.getMainAttributes.getValue("Main-Class")
          val (main, arguments) = if(mainClass != null && !mainClass.equals("") && !mainClass.equals(config.remainArgs(0))) {
            (mainClass, config.remainArgs)
          } else {
            (config.remainArgs(0), config.remainArgs.drop(1))
          }
          val classLoader: URLClassLoader = new URLClassLoader(Array(new URL("file:" + jarFile.getAbsolutePath)),
            Thread.currentThread().getContextClassLoader())

          //set the context classloader as ActorSystem will use context classloader in precedence.
          Thread.currentThread().setContextClassLoader(classLoader)
          val clazz = classLoader.loadClass(main)
          val mainMethod = clazz.getMethod("main", classOf[Array[String]])
          mainMethod.invoke(null, arguments)

        case Some(false) =>
          LOG.info(s"jar $jar does not exist")
        case None =>
          LOG.info("jar file required")
      }
    }).failed.foreach(throwable => {
      val cause = Option(throwable.getCause).getOrElse(throwable)
      Console.println(cause.getMessage)
    })
  }

  start
}
