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
package org.apache.gearpump.scheduler

object ResourceCalculator {
  def clone(res: Resource) = Resource(res.slots)

  def subtract(res1: Resource, res2: Resource) = Resource(res1.slots - res2.slots)

  def add(res1: Resource, res2: Resource) = Resource(res1.slots + res2.slots)

  def compare(res1: Resource, res2: Resource) = res1.slots - res2.slots

  def lessThan(res1: Resource, res2: Resource) = compare(res1, res2) < 0

  def equals(res1: Resource, res2: Resource) = compare(res1, res2) == 0

  def greaterThan(res1: Resource, res2: Resource) = compare(res1, res2) > 0

  def min(res1: Resource, res2: Resource) = if (res1.slots < res2.slots) res1 else res2
}