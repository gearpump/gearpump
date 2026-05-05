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

import io.gearpump.cluster.ClusterConfig;
import org.apache.beam.runners.gearpump.GearpumpPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.pekko.actor.ActorSystem;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/** Tests for low-level Beam-to-Gearpump graph translation. */
public class GearpumpPipelineTranslatorTest {

  private ActorSystem actorSystem;
  private GearpumpPipelineOptions options;

  @Before
  public void setUp() {
    actorSystem = ActorSystem.create("beam-runner-translator-test", ClusterConfig.defaultConfig());
    options = PipelineOptionsFactory.as(GearpumpPipelineOptions.class);
    options.setParallelism(1);
  }

  @After
  public void tearDown() {
    actorSystem.terminate();
  }

  @Test
  public void translatesCreateAndParDoToLowLevelGraph() {
    Pipeline pipeline = Pipeline.create();
    pipeline
        .apply(Create.of("a", "b"))
        .apply("upper", ParDo.of(new UpperCaseFn()));

    TranslationContext context = new TranslationContext("beam-test", options, actorSystem);
    new GearpumpPipelineTranslator(context).translate(pipeline);

    assertEquals(3, context.getGraph().getVertices().size());
    assertEquals(2, context.getGraph().getEdges().size());
  }

  @Test
  public void translatesCreateAndGroupByKeyToLowLevelGraph() {
    Pipeline pipeline = Pipeline.create();
    pipeline
        .apply(Create.of(KV.of("a", 1), KV.of("a", 2)))
        .apply(GroupByKey.create());

    TranslationContext context = new TranslationContext("beam-test", options, actorSystem);
    new GearpumpPipelineTranslator(context).translate(pipeline);

    assertEquals(2, context.getGraph().getVertices().size());
    assertEquals(1, context.getGraph().getEdges().size());
  }

  private static final class UpperCaseFn extends DoFn<String, String> {
    @ProcessElement
    public void processElement(ProcessContext context) {
      context.output(context.element().toUpperCase());
    }
  }
}
