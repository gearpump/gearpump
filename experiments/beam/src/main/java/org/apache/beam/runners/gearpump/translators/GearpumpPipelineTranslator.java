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

import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.io.Read;

/** Translates supported Beam primitives into a low-level Gearpump graph. */
@SuppressWarnings({"rawtypes", "unchecked"})
public class GearpumpPipelineTranslator extends Pipeline.PipelineVisitor.Defaults {

  private static final Map<Class<? extends PTransform>, TransformTranslator> TRANSLATORS =
      new HashMap<>();

  private final TranslationContext translationContext;

  static {
    registerTransformTranslator(Read.Bounded.class, new ReadBoundedTranslator());
    registerTransformTranslator(Flatten.PCollections.class, new FlattenPCollectionsTranslator());
    registerTransformTranslator(GroupByKey.class, new GroupByKeyTranslator());
    registerTransformTranslator(ParDo.MultiOutput.class, new ParDoMultiOutputTranslator());
  }

  public GearpumpPipelineTranslator(TranslationContext translationContext) {
    this.translationContext = translationContext;
  }

  public void translate(Pipeline pipeline) {
    pipeline.traverseTopologically(this);
  }

  @Override
  public CompositeBehavior enterCompositeTransform(TransformHierarchy.Node node) {
    return CompositeBehavior.ENTER_TRANSFORM;
  }

  @Override
  public void visitPrimitiveTransform(TransformHierarchy.Node node) {
    PTransform transform = node.getTransform();
    TransformTranslator translator = getTransformTranslator(transform.getClass());
    if (translator == null) {
      throw new UnsupportedOperationException(
          "Unsupported Beam transform "
              + transform.getClass().getName()
              + ". The low-level Gearpump runner currently supports only "
              + "Read.Bounded/Create, ParDo, Flatten, and GroupByKey in global windows.");
    }
    translationContext.setCurrentTransform(node, getPipeline());
    translator.translate(transform, translationContext);
  }

  private static <TransformT extends PTransform> void registerTransformTranslator(
      Class<TransformT> transformClass,
      TransformTranslator<? extends TransformT> transformTranslator) {
    if (TRANSLATORS.put(transformClass, transformTranslator) != null) {
      throw new IllegalArgumentException("defining multiple translators for " + transformClass);
    }
  }

  private <TransformT extends PTransform> TransformTranslator<TransformT> getTransformTranslator(
      Class<TransformT> transformClass) {
    return TRANSLATORS.get(transformClass);
  }
}
