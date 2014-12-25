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

import java.io.{File, PrintWriter}

import org.apache.gearpump.util.LogUtil
import org.slf4j.{Logger, LoggerFactory}
object Shell extends App with ArgumentsParser {

  private val LOG: Logger = LogUtil.getLogger(getClass)

  override val options: Array[(String, CLIOption[Any])] = Array(
    "master"-> CLIOption("<host1:port1,host2:port2,host3:port3>", required = true))

  val config = parse(args)

  val masters = config.getString("master")
  Console.out.println("Master URL: " + masters)

  def shell() = {
    val java = System.getProperty("java.home") + "/bin/java"
    val scalaHome = System.getenv("SCALA_HOME")
    if (null == scalaHome || "" == scalaHome) {
      LOG.info("Please set SCALA_HOME env")
      System.exit(-1)
    }

    System.setProperty("scala.home", scalaHome)
    System.setProperty("scala.usejavacp", "true")

    System.setProperty("masterActorPath", masters)

    val file = File.createTempFile("scala_shell", ".scala")

    val shell = getClass.getResourceAsStream("/shell.scala")
    val content = scala.io.Source.fromInputStream(shell).mkString
    shell.close()

    val printer = new PrintWriter(file.getAbsolutePath, "UTF-8")
    printer.print(content)
    printer.close()

    scala.tools.nsc.MainGenericRunner.main(Array("-i", file.getAbsolutePath))
  }

  def getClassPath(scalaHome : String) = {
    val classpath = System.getProperty("java.class.path")
    var classpathList = classpath.split(File.pathSeparator)
    classpathList :+= scalaHome + "/lib/*"
    classpathList.mkString(File.pathSeparator)
  }

  shell
}
