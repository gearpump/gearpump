/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.gearpump.streaming.javaapi.kafka;

import io.gearpump.streaming.task.TaskContext;
import org.junit.Test;
import org.mockito.Mockito;
import org.scalatest.junit.JUnitSuite;

public class KafkaSourceSpec extends JUnitSuite {

  @Test
  public void testKafkaSource() {
    System.out.println("testing Java KafkaSource...");

    io.gearpump.streaming.kafka.KafkaSource underlying = Mockito.mock(io.gearpump.streaming.kafka.KafkaSource.class);
    TaskContext context = Mockito.mock(TaskContext.class);
    KafkaSource source = new KafkaSource(underlying);

    final long anyStartTime = 1000L;
    source.open(context, anyStartTime);
    Mockito.verify(underlying).open(context, anyStartTime);

    final int anyBatchSize = 1000;
    source.read(anyBatchSize);
    Mockito.verify(underlying).read(anyBatchSize);

    source.close();
    Mockito.verify(underlying).close();
  }
}
