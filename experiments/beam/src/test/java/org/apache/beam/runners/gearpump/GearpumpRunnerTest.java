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
import io.gearpump.cluster.ClusterConfig;
import io.gearpump.cluster.client.RunningApplication;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import org.apache.beam.runners.gearpump.runtime.TaggedOutputValue;
import org.apache.beam.sdk.PipelineResult;
import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.util.Timeout;
import org.joda.time.Duration;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class GearpumpRunnerTest {

  private static final String GEARPUMP_SERIALIZERS = "gearpump.serializers";

  @Test
  public void configureRunnerConfigPreservesDefaultSerializers() {
    Config config =
        GearpumpRunner.configureRunnerConfig(
            ClusterConfig.defaultConfig(),
            Collections.singletonMap("com.example.CustomValue", ""));

    assertTrue(config.hasPath(GEARPUMP_SERIALIZERS + ".\"[B\""));
    assertTrue(config.hasPath(GEARPUMP_SERIALIZERS + ".\"scala.Tuple2\""));
    assertTrue(config.hasPath(GEARPUMP_SERIALIZERS + ".\"" + TaggedOutputValue.class.getName() + "\""));
    assertTrue(config.hasPath(GEARPUMP_SERIALIZERS + ".\"com.example.CustomValue\""));
  }

  @Test
  public void waitUntilFinishWithTimeoutPassesRequestedDurationToRunningApplication() {
    RecordingRunningApplication app = new RecordingRunningApplication();
    GearpumpPipelineResult result = new GearpumpPipelineResult(null, app);

    PipelineResult.State state = result.waitUntilFinish(Duration.standardSeconds(10));

    assertEquals(PipelineResult.State.DONE, state);
    assertEquals(java.time.Duration.ofSeconds(10), app.duration);
    assertEquals(1, app.durationWaitCalls);
    assertEquals(0, app.unboundedWaitCalls);
  }

  private static final class RecordingRunningApplication extends RunningApplication {

    private int durationWaitCalls;
    private int unboundedWaitCalls;
    private java.time.Duration duration;

    private RecordingRunningApplication() {
      super(1, ActorRef.noSender(), Timeout.apply(1, TimeUnit.SECONDS));
    }

    @Override
    public void waitUntilFinish() {
      unboundedWaitCalls += 1;
    }

    @Override
    public void waitUntilFinish(java.time.Duration duration) {
      durationWaitCalls += 1;
      this.duration = duration;
    }
  }
}
