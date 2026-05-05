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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;

/** Serializable runtime specification for a Beam ParDo task. */
public class BeamParDoFnSpec<InputT, OutputT> implements Serializable {

  private final SerializablePipelineOptions options;
  private final DoFn<InputT, OutputT> doFn;
  private final TupleTag<OutputT> mainOutputTag;
  private final List<TupleTag<?>> sideOutputTags;
  private final Map<TupleTag<?>, Coder<?>> outputCoders;
  private final WindowingStrategy<?, ?> windowingStrategy;
  private final DoFnSchemaInformation doFnSchemaInformation;

  public BeamParDoFnSpec(
      PipelineOptions options,
      DoFn<InputT, OutputT> doFn,
      TupleTag<OutputT> mainOutputTag,
      List<TupleTag<?>> sideOutputTags,
      Map<TupleTag<?>, Coder<?>> outputCoders,
      WindowingStrategy<?, ?> windowingStrategy,
      DoFnSchemaInformation doFnSchemaInformation) {
    this.options = new SerializablePipelineOptions(options);
    this.doFn = doFn;
    this.mainOutputTag = mainOutputTag;
    this.sideOutputTags = new ArrayList<>(sideOutputTags);
    this.outputCoders = new HashMap<>(outputCoders);
    this.windowingStrategy = windowingStrategy;
    this.doFnSchemaInformation = doFnSchemaInformation;
  }

  public PipelineOptions getPipelineOptions() {
    return options.get();
  }

  public DoFn<InputT, OutputT> getDoFn() {
    return doFn;
  }

  public TupleTag<OutputT> getMainOutputTag() {
    return mainOutputTag;
  }

  public List<TupleTag<?>> getSideOutputTags() {
    return sideOutputTags;
  }

  public Map<TupleTag<?>, Coder<?>> getOutputCoders() {
    return outputCoders;
  }

  public WindowingStrategy<?, ?> getWindowingStrategy() {
    return windowingStrategy;
  }

  public DoFnSchemaInformation getDoFnSchemaInformation() {
    return doFnSchemaInformation;
  }
}
