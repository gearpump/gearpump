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

import com.typesafe.config.Config;
import io.gearpump.cluster.ClusterConfig;
import io.gearpump.streaming.Processor;
import io.gearpump.streaming.task.Task;
import org.apache.beam.runners.gearpump.GearpumpRunner;
import org.apache.beam.runners.gearpump.runtime.BeamAssignWindowsTask;
import org.apache.beam.runners.gearpump.runtime.BeamGroupByKeyTask;
import org.apache.beam.runners.gearpump.runtime.BeamParDoTask;
import org.apache.beam.runners.gearpump.runtime.BeamTaggedOutputTask;
import org.apache.beam.runners.gearpump.GearpumpPipelineOptions;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.Coder;
import io.gearpump.streaming.appmaster.ClockService;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.NonMergingWindowFn;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.transforms.windowing.WindowMappingFn;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.pekko.actor.ActorSystem;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.collection.JavaConverters;

import java.util.List;
import java.util.Collection;
import java.util.Collections;
import org.joda.time.Duration;
import org.joda.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/** Tests for low-level Beam-to-Gearpump graph translation. */
public class GearpumpPipelineTranslatorTest {

  private ActorSystem actorSystem;
  private GearpumpPipelineOptions options;

  @BeforeEach
  public void setUp() {
    options = PipelineOptionsFactory.as(GearpumpPipelineOptions.class);
    options.setParallelism(1);
    Config config = GearpumpRunner.configureRunnerConfig(ClusterConfig.defaultConfig(), null);
    actorSystem = ActorSystem.create("beam-runner-translator-test", config);
  }

  @AfterEach
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

    List<Processor<? extends Task>> processors =
        JavaConverters.seqAsJavaListConverter(context.getGraph().getVertices()).asJava();
    assertTrue(processors.size() >= 3);
    assertTrue(context.getGraph().getEdges().size() >= 2);
    assertTrue(containsProcessor(processors, BeamParDoTask.class));
    assertEquals(
        BeamTaggedOutputTask.class, context.getOutputProcessor(context.getOutput()).taskClass());
  }

  @Test
  public void translatesCreateAndGroupByKeyToLowLevelGraph() {
    Pipeline pipeline = Pipeline.create();
    pipeline
        .apply(Create.of(KV.of("a", 1), KV.of("a", 2)))
        .apply(GroupByKey.create());

    TranslationContext context = new TranslationContext("beam-test", options, actorSystem);
    new GearpumpPipelineTranslator(context).translate(pipeline);

    List<Processor<? extends Task>> processors =
        JavaConverters.seqAsJavaListConverter(context.getGraph().getVertices()).asJava();
    assertTrue(processors.size() >= 2);
    assertTrue(context.getGraph().getEdges().size() >= 1);
    assertTrue(containsProcessor(processors, BeamGroupByKeyTask.class));
    assertEquals(
        BeamGroupByKeyTask.class, context.getOutputProcessor(context.getOutput()).taskClass());
  }

  @Test
  public void translatesWindowedGroupByKeyToLowLevelGraph() {
    Pipeline pipeline = Pipeline.create();
    pipeline
        .apply(
            Create.timestamped(
                TimestampedValue.of(KV.of("a", 1), new Instant(0L)),
                TimestampedValue.of(KV.of("a", 2), new Instant(15_000L))))
        .apply(Window.into(FixedWindows.of(Duration.standardSeconds(10))))
        .apply(GroupByKey.create());

    TranslationContext context = new TranslationContext("beam-test", options, actorSystem);
    new GearpumpPipelineTranslator(context).translate(pipeline);

    List<Processor<? extends Task>> processors =
        JavaConverters.seqAsJavaListConverter(context.getGraph().getVertices()).asJava();
    assertTrue(processors.size() >= 3);
    assertTrue(containsProcessor(processors, BeamAssignWindowsTask.class));
    assertTrue(containsProcessor(processors, BeamGroupByKeyTask.class));
    assertEquals(
        BeamGroupByKeyTask.class, context.getOutputProcessor(context.getOutput()).taskClass());
  }

  @Test
  public void translatesUnboundedGroupByKeyInFiniteWindows() {
    Pipeline pipeline = Pipeline.create();
    org.apache.beam.sdk.values.PCollection<KV<String, Integer>> input =
        pipeline.apply(Create.of(KV.of("a", 1)));
    input.setIsBoundedInternal(org.apache.beam.sdk.values.PCollection.IsBounded.UNBOUNDED);
    input.apply(Window.into(FixedWindows.of(Duration.standardSeconds(10))))
        .apply(GroupByKey.create());
    TranslationContext context = new TranslationContext("beam-test", options, actorSystem);
    new GearpumpPipelineTranslator(context).translate(pipeline);
    assertEquals(BeamGroupByKeyTask.class, context.getOutputProcessor(context.getOutput()).taskClass());
  }

  @Test
  public void rejectsUnboundedGlobalWindowGroupingBeforeCreatingState() {
    Pipeline pipeline = Pipeline.create();
    org.apache.beam.sdk.values.PCollection<KV<String, Integer>> input =
        pipeline.apply(Create.of(KV.of("a", 1)));
    input.setIsBoundedInternal(org.apache.beam.sdk.values.PCollection.IsBounded.UNBOUNDED);
    input.apply(Window.<KV<String, Integer>>configure()
            .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(1)))
            .withAllowedLateness(Duration.ZERO).discardingFiredPanes())
        .apply(GroupByKey.create());
    TranslationContext context = new TranslationContext("beam-test", options, actorSystem);
    try {
      new GearpumpPipelineTranslator(context).translate(pipeline);
      fail("Expected unbounded global-window grouping to be rejected");
    } catch (UnsupportedOperationException e) {
      assertTrue(e.getMessage().contains("finite windows"));
    }
    List<Processor<? extends Task>> processors =
        JavaConverters.seqAsJavaListConverter(context.getGraph().getVertices()).asJava();
    assertTrue(!containsProcessor(processors, BeamGroupByKeyTask.class));
  }

  @Test
  public void rejectsCustomGlobalWindowFunctionOnUnboundedInput() {
    Pipeline pipeline = Pipeline.create();
    org.apache.beam.sdk.values.PCollection<KV<String, Integer>> input =
        pipeline.apply(Create.of(KV.of("a", 1)));
    input.setIsBoundedInternal(org.apache.beam.sdk.values.PCollection.IsBounded.UNBOUNDED);
    input.apply(Window.into(new CustomGlobalWindows())).apply(GroupByKey.create());
    TranslationContext context = new TranslationContext("beam-test", options, actorSystem);
    try {
      new GearpumpPipelineTranslator(context).translate(pipeline);
      fail("Expected custom global-window grouping to be rejected");
    } catch (UnsupportedOperationException e) {
      assertTrue(e.getMessage().contains("FixedWindows or SlidingWindows"));
    }
    List<Processor<? extends Task>> processors =
        JavaConverters.seqAsJavaListConverter(context.getGraph().getVertices()).asJava();
    assertTrue(!containsProcessor(processors, BeamGroupByKeyTask.class));
  }

  @Test
  public void groupingRequiresFullReplayOfVolatileState() {
    Pipeline pipeline = Pipeline.create();
    pipeline.apply(Create.of(KV.of("a", 1))).apply(GroupByKey.create());
    TranslationContext context = new TranslationContext("beam-test", options, actorSystem);
    new GearpumpPipelineTranslator(context).translate(pipeline);
    assertTrue(Boolean.TRUE.equals(context.getOutputProcessor(context.getOutput()).taskConf()
        .getBoolean(ClockService.REPLAY_FROM_START()).get()));
  }

  @Test
  public void rejectsGroupByKeyWithCustomTrigger() {
    Pipeline pipeline = Pipeline.create();
    pipeline
        .apply(
            Create.timestamped(
                TimestampedValue.of(KV.of("a", 1), new Instant(0L)))
                .withCoder(
                    KvCoder.of(
                        org.apache.beam.sdk.coders.StringUtf8Coder.of(),
                        org.apache.beam.sdk.coders.VarIntCoder.of())))
        .apply(
            Window.<KV<String, Integer>>into(FixedWindows.of(Duration.standardSeconds(10)))
                .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(1)))
                .withAllowedLateness(Duration.ZERO)
                .discardingFiredPanes())
        .apply(GroupByKey.create());

    TranslationContext context = new TranslationContext("beam-test", options, actorSystem);
    try {
      new GearpumpPipelineTranslator(context).translate(pipeline);
      fail("Expected custom trigger support to be rejected");
    } catch (UnsupportedOperationException e) {
      assertTrue(e.getMessage().contains("default trigger/pane semantics"));
    }
  }

  @Test
  public void translatesKeyedCombineToLowLevelGraph() {
    Pipeline pipeline = Pipeline.create();
    pipeline
        .apply(Create.of(KV.of("a", 1), KV.of("a", 2), KV.of("b", 5)))
        .apply(GroupByKey.create())
        .apply(Combine.groupedValues(Sum.ofIntegers()));

    TranslationContext context = new TranslationContext("beam-test", options, actorSystem);
    new GearpumpPipelineTranslator(context).translate(pipeline);

    List<Processor<? extends Task>> processors =
        JavaConverters.seqAsJavaListConverter(context.getGraph().getVertices()).asJava();
    assertTrue(processors.size() >= 2);
    assertTrue(containsProcessor(processors, BeamGroupByKeyTask.class));
    assertNotNull(context.getOutputProcessor(context.getOutput()));
  }

  private static boolean containsProcessor(
      List<Processor<? extends Task>> processors, Class<? extends Task> taskClass) {
    for (Processor<? extends Task> processor : processors) {
      if (taskClass.equals(processor.taskClass())) {
        return true;
      }
    }
    return false;
  }

  private static final class CustomGlobalWindows
      extends NonMergingWindowFn<KV<String, Integer>, GlobalWindow> {
    @Override
    public Collection<GlobalWindow> assignWindows(AssignContext context) {
      return Collections.singletonList(GlobalWindow.INSTANCE);
    }

    @Override
    public Coder<GlobalWindow> windowCoder() {
      return GlobalWindow.Coder.INSTANCE;
    }

    @Override
    public boolean isCompatible(WindowFn<?, ?> other) {
      return other instanceof CustomGlobalWindows;
    }

    @Override
    public WindowMappingFn<GlobalWindow> getDefaultWindowMappingFn() {
      return new GlobalWindows().getDefaultWindowMappingFn();
    }
  }

  private static final class UpperCaseFn extends DoFn<String, String> {
    @ProcessElement
    public void processElement(ProcessContext context) {
      context.output(context.element().toUpperCase());
    }
  }
}
