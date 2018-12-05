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
package io.gearpump

/**
 * Types and constants of time in gearpump
 */
object Time {
  type MilliSeconds = Long

  // maximum valid time that won't overflow when being converted to milli-seconds
  // Long.MaxValue is reserved for unreachable time
  val MAX_TIME_MILLIS: Long = Long.MaxValue - 1

  // minimum valid time won't overflow when being converted to milli-seconds
  val MIN_TIME_MILLIS: Long = Long.MinValue

  val UNREACHABLE: Long = Long.MaxValue
}
