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
package org.apache.beam.runners.gearpump.runtime;

import java.io.Serializable;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.values.WindowingStrategy;

/** Serializable runtime specification for Beam window assignment. */
public class BeamAssignWindowsSpec<T> implements Serializable {

  private final WindowFn<T, ? extends BoundedWindow> windowFn;
  private final WindowingStrategy<?, ?> windowingStrategy;

  public BeamAssignWindowsSpec(
      WindowFn<T, ? extends BoundedWindow> windowFn, WindowingStrategy<?, ?> windowingStrategy) {
    this.windowFn = windowFn;
    this.windowingStrategy = windowingStrategy;
  }

  public WindowFn<T, ? extends BoundedWindow> getWindowFn() {
    return windowFn;
  }

  public WindowingStrategy<?, ?> getWindowingStrategy() {
    return windowingStrategy;
  }
}
