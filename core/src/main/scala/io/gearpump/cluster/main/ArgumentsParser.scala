/*
 * Licensed under the Apache License, Version 2.0 (the
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

package io.gearpump.cluster.main

import io.gearpump.cluster.main.ArgumentsParser.Syntax

case class CLIOption[+T](
    description: String = "", required: Boolean = false, defaultValue: Option[T] = None)

class ParseResult(optionMap: Map[String, String], remainArguments: Array[String]) {
  def getInt(key: String): Int = optionMap(key).toInt

  def getString(key: String): String = optionMap(key)

  def getBoolean(key: String): Boolean = optionMap(key).toBoolean

  def exists(key: String): Boolean = !optionMap.getOrElse(key, "").isEmpty

  def remainArgs: Array[String] = this.remainArguments
}

/**
 * Parser for command line arguments
 *
 * Grammar: -option1 value1 -option2 value3 -flag1 -flag2 remainArg1 remainArg2...
 */
trait ArgumentsParser {

  val ignoreUnknownArgument = false

  // scalastyle:off println
  def help(): Unit = {
    Console.println(s"\nHelp: $description")
    val usage = options.map(kv =>
      if (kv._2.required) {
        s"-${kv._1} (required:${kv._2.required})${kv._2.description}"
      } else {
        s"-${kv._1} (required:${kv._2.required}, " +
          s"default:${kv._2.defaultValue.getOrElse("")})${kv._2.description}"
      }) :+ (remainArgs.map(k => s"<$k>").mkString(" "))
    usage.foreach(Console.println)
  }
  // scalastyle:on println

  def parse(args: Array[String]): ParseResult = {
    val syntax = Syntax(options, remainArgs, ignoreUnknownArgument)
    ArgumentsParser.parse(syntax, args)
  }

  val description: String = ""
  val options: Array[(String, CLIOption[Any])] = Array.empty[(String, CLIOption[Any])]
  val remainArgs: Array[String] = Array.empty[String]
}

object ArgumentsParser {

  case class Syntax(
      options: Array[(String, CLIOption[Any])],
      remainArgs: Array[String],
      ignoreUnknownArgument: Boolean)

  def parse(syntax: Syntax, args: Array[String]): ParseResult = {
    import syntax.{ignoreUnknownArgument, options, remainArgs}
    var config = Map.empty[String, String]
    var remain = Array.empty[String]

    @annotation.tailrec
    def doParse(argument: List[String]): Unit = {
      argument match {
        case Nil => Unit // true if everything processed successfully

        case key :: value :: rest if key.startsWith("-") && !value.startsWith("-") =>
          val fixedKey = key.substring(1)
          if (!options.map(_._1).contains(fixedKey)) {
            if (!ignoreUnknownArgument) {
              throw new Exception(s"found unknown option $fixedKey")
            } else {
              remain ++= Array(key, value)
            }
          } else {
            config += fixedKey -> value
          }
          doParse(rest)

        case key :: rest if key.startsWith("-") =>
          val fixedKey = key.substring(1)
          if (!options.map(_._1).contains(fixedKey)) {
            throw new Exception(s"found unknown option $fixedKey")
          } else {
            config += fixedKey -> "true"
          }
          doParse(rest)

        case value :: rest =>
          remain ++= value :: rest
          doParse(Nil)
      }
    }
    doParse(args.toList)

    options.foreach { pair =>
      val (key, option) = pair
      if (!config.contains(key) && !option.required) {
        config += key -> option.defaultValue.getOrElse("").toString
      }
    }

    options.foreach { case (key, _) =>
      if (config.get(key).isEmpty) {
        throw new Exception(s"Missing option $key...")
      }
    }

    if (remain.length < remainArgs.length) {
      throw new Exception(s"Missing arguments ...")
    }

    new ParseResult(config, remain)
  }
}