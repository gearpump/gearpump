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
package io.gearpump.streaming.dsl.api.functions

/**
 * Superclass for all user defined function interfaces.
 * This ensures all functions are serializable and provides common methods
 * like setup and teardown. Users should not extend this class directly
 * but subclasses like [[io.gearpump.streaming.dsl.scalaapi.functions.FlatMapFunction]].
 */
abstract class SerializableFunction extends java.io.Serializable {

  def setup(): Unit = {}

  def teardown(): Unit = {}

}
