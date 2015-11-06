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

package akka.stream.gearpump

import akka.stream.ActorAttributes.{SupervisionStrategy, Dispatcher}
import akka.stream.Attributes
import akka.stream.Attributes.{Name, LogLevels, InputBuffer, Attribute}

object GearAttributes {

  /**
   * Define how many parallel instance we want to use to run this module
   * @param count
   * @return
   */
  def count(count: Int): Attributes =  Attributes(ParallismAttribute(count))

  /**
   * Define we want to render this module locally.
   * @return
   */
  def local: Attributes = Attributes(LocationAttribute(Local))

  /**
   * Define we want to render this module remotely
   * @return
   */
  def remote: Attributes = Attributes(LocationAttribute(Remote))

  /**
   * Get the effective location settings if child override the parent
   * setttings.
   *
   * @param attrs
   * @return
   */
  def location(attrs: Attributes): Location = {
    attrs.attributeList.foldLeft(Local: Location) { (s, attr) =>
      attr match {
        case LocationAttribute(location)    => location
        case other => s
      }
    }
  }

  /**
   * get effective parallelism settings if child override parent.
   * @param attrs
   * @return
   */
  def count(attrs: Attributes): Int = {
    attrs.attributeList.foldLeft(1) { (s, attr) =>
      attr match {
        case ParallismAttribute(count)    => count
        case other => s
      }
    }
  }

  /**
   * Where we want to render the module
   */
  sealed trait Location
  object Local extends Location
  object Remote extends Location

  final case class LocationAttribute(tag: Location) extends Attribute

  /**
   * How many parallel instance we want to use for this module.
   *
   * @param parallelism
   */
  final case class ParallismAttribute(parallelism: Int) extends Attribute
}
