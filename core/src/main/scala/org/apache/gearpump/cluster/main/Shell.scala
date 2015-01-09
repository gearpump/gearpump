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
object Shell extends App with ArgumentsParser {

  case object ShellStarted

  override val options: Array[(String, CLIOption[Any])] = Array(
    "master"-> CLIOption("<host1:port1,host2:port2,host3:port3>", required = true))


  def shell() : Unit = {
    val config = parse(args)
    if (null == config) {
      return
    }

    val masters = config.getString("master")
    Console.out.println("Master URL: " + masters)


    val java = System.getProperty("java.home") + "/bin/java"
    val scalaHome = System.getenv("SCALA_HOME")
    if (null != scalaHome) {
      System.setProperty("scala.home", scalaHome)
    }
    System.setProperty("scala.usejavacp", "true")

    System.setProperty("masterActorPath", masters)

    val file = File.createTempFile("scala_shell", ".scala")
    Console.out.println()

    val shell = Option(getClass.getResourceAsStream("/shell.scala"))

    val content = shell match {
      case Some(stream) => {
        val data = scala.io.Source.fromInputStream (stream).mkString
        stream.close()
        data
      }
      case None => ""
    }

    val printer = new PrintWriter(file.getAbsolutePath, "UTF-8")
    printer.print(content)
    printer.close()

    Console.out.println(s"Starting shell file " + file.getAbsolutePath)

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
