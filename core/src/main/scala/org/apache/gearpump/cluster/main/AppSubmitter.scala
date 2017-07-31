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
package org.apache.gearpump.cluster.main

import java.io.File
import java.net.{URL, URLClassLoader}
import java.util.jar.JarFile

import org.apache.gearpump.cluster.client.{RemoteRuntimeEnvironment, RuntimeEnvironment}
import org.apache.gearpump.util.{AkkaApp, Constants, LogUtil, Util}

import scala.util.{Failure, Success, Try}

/** Tool to submit an application jar to cluster */
object AppSubmitter extends AkkaApp with ArgumentsParser {

  override val ignoreUnknownArgument = true

  override val description = "Submit an application to Master by providing a jar"

  override val options: Array[(String, CLIOption[Any])] = Array(
    "namePrefix" -> CLIOption[String]("<application name prefix>", required = false,
      defaultValue = Some("")),
    "jar" -> CLIOption("<application>.jar", required = true),
    "executors" -> CLIOption[Int]("number of executor to launch", required = false,
      defaultValue = Some(1)),
    "verbose" -> CLIOption("<print verbose log on console>", required = false,
      defaultValue = Some(false)),
    // For document purpose only, OPTION_CONFIG option is not used here.
    // OPTION_CONFIG is parsed by parent shell command "Gear" transparently.
    Gear.OPTION_CONFIG -> CLIOption("custom configuration file", required = false,
      defaultValue = None))

  def main(akkaConf: Config, args: Array[String]): Unit = {

    val config = parse(args)
    if (null != config) {

      val verbose = config.getBoolean("verbose")
      if (verbose) {
        LogUtil.verboseLogToConsole()
      }

      val jar = config.getString("jar")

      // Set jar path to be submitted to cluster
      System.setProperty(Constants.GEARPUMP_APP_JAR, jar)
      System.setProperty(Constants.APPLICATION_EXECUTOR_NUMBER, config.getInt("executors").toString)

      val namePrefix = config.getString("namePrefix")
      if (namePrefix.nonEmpty) {
        if (!Util.validApplicationName(namePrefix)) {
          throw new Exception(s"$namePrefix is not a valid prefix for an application name")
        }
        System.setProperty(Constants.GEARPUMP_APP_NAME_PREFIX, namePrefix)
      }

      val jarFile = new java.io.File(jar)

      // Start main class
      if (!jarFile.exists()) {
        throw new Exception(s"jar $jar does not exist")
      }

      val classLoader: URLClassLoader = new URLClassLoader(Array(new URL("file:" +
        jarFile.getAbsolutePath)), Thread.currentThread().getContextClassLoader)
      val (main, arguments) = parseMain(jarFile, config.remainArgs, classLoader)

      // Set to context classLoader. ActorSystem pick context classLoader in preference
      Thread.currentThread().setContextClassLoader(classLoader)
      val clazz = classLoader.loadClass(main)
      val mainMethod = clazz.getMethod("main", classOf[Array[String]])
      RuntimeEnvironment.setRuntimeEnv(new RemoteRuntimeEnvironment)
      mainMethod.invoke(null, arguments)
    }
  }

  private def parseMain(jar: File, remainArgs: Array[String],
      classLoader: ClassLoader): (String, Array[String]) = {
    if (remainArgs.nonEmpty && Try(classLoader.loadClass(remainArgs.head)).isSuccess) {
      (remainArgs.head, remainArgs.tail)
    } else {
      val mainInManifest =
        new JarFile(jar).getManifest.getMainAttributes.getValue("Main-Class")
      val args = remainArgs.mkString(" ")
      Try(classLoader.loadClass(mainInManifest)) match {
        case Success(_) =>
          // scalastyle:off println
          Console.println(
            s"""Can't load main class in arguments $args
              |Loading $mainInManifest in manifest""".stripMargin)
          // scalastyle:on println
          (mainInManifest, remainArgs)
        case Failure(_) =>
          throw new IllegalArgumentException(
            s"Can't load main class in arguments $args or $mainInManifest in manifest")
      }
    }
  }
}
