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
package org.apache.beam.runners.gearpump;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueFactory;
import io.gearpump.cluster.ClusterConfig;
import io.gearpump.cluster.UserConfig;
import io.gearpump.cluster.client.BeamClientContext;
import io.gearpump.cluster.client.ClientContext;
import io.gearpump.cluster.client.RunningApplication;
import io.gearpump.streaming.javaapi.StreamApplication;
import io.gearpump.util.Constants;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.beam.runners.gearpump.runtime.TaggedOutputValue;
import org.apache.beam.runners.gearpump.translators.GearpumpPipelineTranslator;
import org.apache.beam.runners.gearpump.translators.TranslationContext;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.apache.pekko.actor.ActorSystem;

/**
 * A {@link PipelineRunner} that executes the supported parts of a Beam pipeline on Gearpump's
 * low-level {@code Processor}/{@code Task} graph API.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class GearpumpRunner extends PipelineRunner<GearpumpPipelineResult> {

  private static final String GEARPUMP_SERIALIZERS = "gearpump.serializers";
  private static final String DEFAULT_APPNAME = "beam_gearpump_app";

  private final GearpumpPipelineOptions options;

  public GearpumpRunner(GearpumpPipelineOptions options) {
    this.options = options;
  }

  public static GearpumpRunner fromOptions(PipelineOptions options) {
    GearpumpPipelineOptions pipelineOptions =
        PipelineOptionsValidator.validate(GearpumpPipelineOptions.class, options);
    return new GearpumpRunner(pipelineOptions);
  }

  @Override
  public GearpumpPipelineResult run(Pipeline pipeline) {
    String appName = options.getApplicationName();
    if (appName == null) {
      appName = DEFAULT_APPNAME;
    }

    Config config = registerSerializers(ClusterConfig.defaultConfig(), options.getSerializers());
    if (!options.getRemote()) {
      config =
          config.withValue(Constants.APPLICATION_TOTAL_RETRIES(), ConfigValueFactory.fromAnyRef(0));
    }

    ActorSystem translationSystem =
        ActorSystem.create("beamTranslator-" + UUID.randomUUID().toString(), config);
    try {
      TranslationContext translationContext =
          new TranslationContext(appName, options, translationSystem);
      GearpumpPipelineTranslator translator = new GearpumpPipelineTranslator(translationContext);
      translator.translate(pipeline);

      ClientContext clientContext = BeamClientContext.create(config, options.getRemote());
      options.setClientContext(clientContext);

      StreamApplication app =
          new StreamApplication(appName, UserConfig.empty(), translationContext.getGraph());
      RunningApplication running = clientContext.submit(app);
      return new GearpumpPipelineResult(clientContext, running);
    } finally {
      translationSystem.terminate();
    }
  }

  /** Register Beam runtime classes with the default Gearpump serializers. */
  private Config registerSerializers(Config config, Map<String, String> userSerializers) {
    Map<String, String> serializers = new HashMap<>();
    serializers.put("org.apache.beam.sdk.util.WindowedValue$ValueInGlobalWindow", "");
    serializers.put("org.apache.beam.sdk.util.WindowedValue$TimestampedValueInSingleWindow", "");
    serializers.put("org.apache.beam.sdk.util.WindowedValue$TimestampedValueInGlobalWindow", "");
    serializers.put("org.apache.beam.sdk.util.WindowedValue$TimestampedValueInMultipleWindows", "");
    serializers.put("org.apache.beam.sdk.transforms.windowing.PaneInfo", "");
    serializers.put("org.apache.beam.sdk.transforms.windowing.PaneInfo$Timing", "");
    serializers.put("org.apache.beam.sdk.transforms.windowing.IntervalWindow", "");
    serializers.put("org.apache.beam.sdk.values.KV", "");
    serializers.put("org.apache.beam.sdk.values.TimestampedValue", "");
    serializers.put("org.joda.time.Instant", "");
    serializers.put(TaggedOutputValue.class.getName(), "");

    if (userSerializers != null && !userSerializers.isEmpty()) {
      serializers.putAll(userSerializers);
    }

    return config.withValue(GEARPUMP_SERIALIZERS, ConfigValueFactory.fromMap(serializers));
  }
}
