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
import io.gearpump.streaming.source.DataSourceTask;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.runners.gearpump.translators.io.CreateValuesSource;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;

/** Translates Beam {@link Create.Values} without relying on Beam's SDF expansion. */
public class CreateValuesTranslator<T> implements TransformTranslator<Create.Values<T>> {

  @Override
  public void translate(Create.Values<T> transform, TranslationContext context) {
    PCollection<T> output = (PCollection<T>) context.getOutput();
    Coder<T> coder = output.getCoder();
    List<T> values = new ArrayList<>();
    for (T value : transform.getElements()) {
      values.add(value);
    }

    Processor<DataSourceTask> source =
        Processor.source(
            new CreateValuesSource<>(values, coder),
            context.getParallelism(),
            transform.getName(),
            UserConfig.empty(),
            context.getActorSystem());
    context.getGraph().addVertex(source);
    context.setOutputProcessor(output, source);
  }
}
