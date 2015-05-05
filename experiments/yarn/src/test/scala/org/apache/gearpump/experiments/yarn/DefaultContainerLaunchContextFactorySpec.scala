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

package org.apache.gearpump.experiments.yarn

import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.scalatest.FlatSpecLike
import org.scalatest.Matchers._
import org.specs2.mock.Mockito


class DefaultContainerLaunchContextFactorySpec extends FlatSpecLike
with Mockito {

  "A DefaultContainerLaunchContextFactory" should "create new ContainerLaunchContext object when newInstance is called on it" in {
    DefaultContainerLaunchContextFactory(mock[YarnConfiguration], mock[AppConfig]) should not be theSameInstanceAs(DefaultContainerLaunchContextFactory(mock[YarnConfiguration], mock[AppConfig]))
  }
}
