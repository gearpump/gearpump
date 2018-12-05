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
package io.gearpump.streaming.dsl.window.api

/**
 * Determines when window results are emitted.
 * For now, [[EventTimeTrigger]] is used for all applications.
 */
// TODO: Make this a public API
sealed trait Trigger

/**
 * Triggers emitting when watermark past the end of window on event time.
 */
// FIXME: This is no more than a tag now and the logic is hard corded in WindowRunner
case object EventTimeTrigger extends Trigger

