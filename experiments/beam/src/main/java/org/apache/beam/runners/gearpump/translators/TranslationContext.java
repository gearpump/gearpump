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
package org.apache.beam.runners.gearpump.translators;

import io.gearpump.cluster.UserConfig;
import io.gearpump.streaming.javaapi.Graph;
import io.gearpump.streaming.javaapi.Processor;
import io.gearpump.streaming.partitioner.Partitioner;
import io.gearpump.streaming.task.Task;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.beam.runners.gearpump.GearpumpPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.util.construction.TransformInputs;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.pekko.actor.ActorSystem;

/** Maintains context data for the low-level Gearpump Beam translators. */
@SuppressWarnings({"rawtypes", "unchecked"})
public class TranslationContext {

  private final String appName;
  private final GearpumpPipelineOptions pipelineOptions;
  private final ActorSystem actorSystem;
  private final Graph graph = new Graph();
  private AppliedPTransform<?, ?, ?> currentTransform;
  private final Map<PValue, Processor<? extends Task>> processors = new HashMap<>();

  public TranslationContext(
      String appName, GearpumpPipelineOptions pipelineOptions, ActorSystem actorSystem) {
    this.appName = appName;
    this.pipelineOptions = pipelineOptions;
    this.actorSystem = actorSystem;
  }

  public void setCurrentTransform(TransformHierarchy.Node treeNode, Pipeline pipeline) {
    this.currentTransform = treeNode.toAppliedPTransform(pipeline);
  }

  public GearpumpPipelineOptions getPipelineOptions() {
    return pipelineOptions;
  }

  public ActorSystem getActorSystem() {
    return actorSystem;
  }

  public String getAppName() {
    return appName;
  }

  public Graph getGraph() {
    return graph;
  }

  public int getParallelism() {
    return pipelineOptions.getParallelism();
  }

  public <T extends Task> Processor<T> addProcessor(
      Class<T> taskClass, UserConfig taskConf, String description) {
    Processor<T> processor = new Processor<>(taskClass, getParallelism(), description, taskConf);
    graph.addVertex(processor);
    return processor;
  }

  public <T extends Task> void connect(
      PValue input, Partitioner partitioner, Processor<T> destinationProcessor) {
    graph.addVertexAndEdge(getOutputProcessor(input), partitioner, destinationProcessor);
  }

  public <T extends Task> void setOutputProcessor(
      PValue output, Processor<T> outputProcessor) {
    if (!processors.containsKey(output)) {
      processors.put(output, outputProcessor);
    } else {
      throw new IllegalStateException("set processor for duplicated output " + output);
    }
  }

  public <T extends Task> Processor<T> getOutputProcessor(PValue input) {
    return (Processor<T>) processors.get(input);
  }

  @SuppressWarnings("unchecked")
  public Map<TupleTag<?>, PValue> getInputs() {
    return (Map<TupleTag<?>, PValue>) (Map<?, ?>) getCurrentTransform().getInputs();
  }

  public PValue getInput() {
    return getOnlyElement(TransformInputs.nonAdditionalInputs(getCurrentTransform()));
  }

  @SuppressWarnings("unchecked")
  public Map<TupleTag<?>, PValue> getOutputs() {
    return (Map<TupleTag<?>, PValue>) (Map<?, ?>) getCurrentTransform().getOutputs();
  }

  public PValue getOutput() {
    return getOnlyElement(getOutputs().values());
  }

  public AppliedPTransform<?, ?, ?> getCurrentTransform() {
    if (currentTransform == null) {
      throw new IllegalStateException("current transform not set");
    }
    return currentTransform;
  }

  private static <T> T getOnlyElement(Collection<T> values) {
    Iterator<T> iterator = values.iterator();
    if (!iterator.hasNext()) {
      throw new IllegalStateException("expected one element but collection was empty");
    }
    T value = iterator.next();
    if (iterator.hasNext()) {
      throw new IllegalStateException("expected exactly one element");
    }
    return value;
  }
}
