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
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.CombineFnBase;

/** Serializable runtime specification for Beam keyed combine-on-grouped-values. */
public class BeamCombineGroupedValuesSpec<K, InputT, OutputT> implements Serializable {

  private final SerializablePipelineOptions options;
  private final CombineFnBase.GlobalCombineFn<? super InputT, ?, OutputT> combineFn;

  public BeamCombineGroupedValuesSpec(
      PipelineOptions options, CombineFnBase.GlobalCombineFn<? super InputT, ?, OutputT> combineFn) {
    this.options = new SerializablePipelineOptions(options);
    this.combineFn = combineFn;
  }

  public PipelineOptions getPipelineOptions() {
    return options.get();
  }

  public CombineFnBase.GlobalCombineFn<? super InputT, ?, OutputT> getCombineFn() {
    return combineFn;
  }
}
