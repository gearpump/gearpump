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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.core.construction.ParDoTranslation;
import org.apache.beam.runners.gearpump.runtime.BeamParDoFnSpec;
import org.apache.beam.runners.gearpump.runtime.BeamParDoTask;
import org.apache.beam.runners.gearpump.runtime.BeamTaggedOutputTask;
import org.apache.beam.runners.gearpump.runtime.BeamUserConfig;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.DoFnSchemaInformation;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;

/** Translates Beam {@link ParDo.MultiOutput} into a low-level Gearpump DoFn task. */
@SuppressWarnings({"rawtypes", "unchecked"})
public class ParDoMultiOutputTranslator<InputT, OutputT>
    implements TransformTranslator<ParDo.MultiOutput<InputT, OutputT>> {

  @Override
  public void translate(ParDo.MultiOutput<InputT, OutputT> transform, TranslationContext context) {
    if (!transform.getSideInputs().isEmpty()) {
      throw new UnsupportedOperationException(
          "The low-level Gearpump Beam runner does not support side inputs yet.");
    }

    PCollection<InputT> input = (PCollection<InputT>) context.getInput();
    Map<TupleTag<?>, PValue> outputs = context.getOutputs();
    TupleTag<OutputT> mainOutputTag = transform.getMainOutputTag();
    List<TupleTag<?>> sideOutputTags = new ArrayList<>();
    Map<TupleTag<?>, Coder<?>> outputCoders = new HashMap<>();

    for (Map.Entry<TupleTag<?>, PValue> output : outputs.entrySet()) {
      TupleTag<?> outputTag = output.getKey();
      if (outputTag != null && !outputTag.getId().equals(mainOutputTag.getId())) {
        sideOutputTags.add(outputTag);
      }
      if (output.getValue() instanceof PCollection) {
        TupleTag<?> coderTag = outputTag == null ? mainOutputTag : outputTag;
        outputCoders.put(coderTag, ((PCollection<?>) output.getValue()).getCoder());
      }
    }

    DoFnSchemaInformation doFnSchemaInformation =
        ParDoTranslation.getSchemaInformation(context.getCurrentTransform());
    BeamParDoFnSpec<InputT, OutputT> spec =
        new BeamParDoFnSpec<>(
            context.getPipelineOptions(),
            transform.getFn(),
            mainOutputTag,
            sideOutputTags,
            outputCoders,
            input.getWindowingStrategy(),
            doFnSchemaInformation);

    UserConfig parDoConfig =
        BeamUserConfig.withValue(
            UserConfig.empty(), BeamParDoTask.PAR_DO_SPEC, spec, context.getActorSystem());
    Processor<BeamParDoTask> parDo =
        context.addProcessor(BeamParDoTask.class, parDoConfig, transform.getName());
    context.connect(context.getInput(), new CoLocationPartitioner(), parDo);

    for (Map.Entry<TupleTag<?>, PValue> output : outputs.entrySet()) {
      String outputTagId =
          output.getKey() == null ? mainOutputTag.getId() : output.getKey().getId();
      UserConfig selectorConfig = UserConfig.empty().withString(BeamTaggedOutputTask.OUTPUT_TAG, outputTagId);
      Processor<BeamTaggedOutputTask> selector =
          context.addProcessor(
              BeamTaggedOutputTask.class,
              selectorConfig,
              transform.getName() + ":" + outputTagId);
      context.getGraph().addVertexAndEdge(parDo, new CoLocationPartitioner(), selector);
      context.setOutputProcessor(output.getValue(), selector);
    }
  }
}
