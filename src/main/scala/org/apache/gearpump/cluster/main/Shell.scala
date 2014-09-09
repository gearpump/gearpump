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
import org.apache.gearpump.util.Configs
import org.slf4j.{Logger, LoggerFactory}
import org.apache.gearpump.util.Constants._
object Shell extends App with ArgumentsParser {

  private val LOG: Logger = LoggerFactory.getLogger(Local.getClass)

  override val options = Array("ip"-> "master ip", "port"-> "master port")

  val config = parse(args)

  val ip = config.getString("ip")
  val port = config.getInt("port")
  val masterURL = s"akka.tcp://${MASTER}@$ip:$port/user/${MASTER}"
  Console.out.println("Master URL: " + masterURL)

  def start() = {
    val java = System.getenv("JAVA_HOME") + "/bin/java"
    val scalaHome = System.getenv("SCALA_HOME")
    if (null == scalaHome || "" == scalaHome) {
      Console.println("Please set SCALA_HOME env")
      System.exit(-1)
    }

    System.setProperty("scala.home", scalaHome)
    System.setProperty("scala.usejavacp", "true")

    System.setProperty("masterActorPath", masterURL)

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
    val classpath = System.getProperty("java.class.path");
    var classpathList = classpath.split(File.pathSeparator);
    classpathList :+= scalaHome + "/lib/*"
    classpathList.mkString(File.pathSeparator)
  }

  start
}
