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
import io.gearpump.streaming.javaapi.Processor;
import io.gearpump.streaming.partitioner.CoLocationPartitioner;
import org.apache.beam.runners.gearpump.runtime.BeamFlattenTask;
import org.apache.beam.runners.gearpump.translators.io.EmptySource;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.values.PValue;
import io.gearpump.streaming.source.DataSourceTask;

/** Translates Beam {@link Flatten.PCollections} into a low-level Gearpump union node. */
public class FlattenPCollectionsTranslator<T>
    implements TransformTranslator<Flatten.PCollections<T>> {

  @Override
  public void translate(Flatten.PCollections<T> transform, TranslationContext context) {
    if (context.getInputs().isEmpty()) {
      Processor<DataSourceTask> source =
          Processor.source(
              new EmptySource(),
              1,
              transform.getName(),
              UserConfig.empty(),
              context.getActorSystem());
      context.getGraph().addVertex(source);
      context.setOutputProcessor(context.getOutput(), source);
      return;
    }

    if (context.getInputs().size() == 1) {
      Processor<?> inputProcessor = context.getOutputProcessor(context.getInput());
      context.setOutputProcessor(context.getOutput(), (Processor) inputProcessor);
      return;
    }

    Processor<BeamFlattenTask> flatten =
        context.addProcessor(BeamFlattenTask.class, UserConfig.empty(), transform.getName());
    for (PValue input : context.getInputs().values()) {
      context.connect(input, new CoLocationPartitioner(), flatten);
    }
    context.setOutputProcessor(context.getOutput(), flatten);
  }
}
