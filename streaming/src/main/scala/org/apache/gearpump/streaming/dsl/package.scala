/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gearpump.streaming


// scalastyle:off line.size.limit
/**
 *
 * The architecture of Gearpump Streaming DSL consists of several layers:
 *
 *   * User facing [[org.apache.gearpump.streaming.dsl.scalaapi.Stream]] DSL. Stream is created by [[org.apache.gearpump.streaming.dsl.scalaapi.StreamApp]]
 *     from input source like Kafka or by applying high level operations (e.g. flatMap, window, groupBy) to user defined functions(UDFs). UDFs are subclasses
 *     of [[org.apache.gearpump.streaming.dsl.api.functions.SerializableFunction]], represented by [[org.apache.gearpump.streaming.dsl.plan.Op]]
 *     in the underlying [[org.apache.gearpump.util.Graph]].
 *   * [[org.apache.gearpump.streaming.dsl.plan.Planner]], responsible for interpreting the Op Graph, optimizing it and building a low level Graph of
 *     [[org.apache.gearpump.streaming.Processor]]. Finally, it creates a runnable Graph of [[org.apache.gearpump.streaming.task.Task]].
 *   * The execution layer is usually composed of the following four tasks.
 *
 *     * [[org.apache.gearpump.streaming.source.DataSourceTask]] for [[org.apache.gearpump.streaming.source.DataSource]] to ingest data into Gearpump
 *     * [[org.apache.gearpump.streaming.sink.DataSinkTask]] for [[org.apache.gearpump.streaming.sink.DataSink]] to write data out.
 *     * [[org.apache.gearpump.streaming.dsl.task.GroupByTask]] to execute Ops followed by [[org.apache.gearpump.streaming.dsl.plan.GroupByOp]]
 *     * [[org.apache.gearpump.streaming.dsl.task.TransformTask]] to execute all other Ops.
 *
 *     All but [[org.apache.gearpump.streaming.sink.DataSinkTask]] delegates execution to [[org.apache.gearpump.streaming.dsl.window.impl.StreamingOperator]], which internally
 *     runs a chain of [[org.apache.gearpump.streaming.dsl.plan.functions.FunctionRunner]] grouped by windows. Window assignments are either explicitly defined with
 *     [[org.apache.gearpump.streaming.dsl.window.api.Windows]] API or implicitly in [[org.apache.gearpump.streaming.dsl.window.api.GlobalWindows]]. UDFs are eventually
 *     executed by [[org.apache.gearpump.streaming.dsl.plan.functions.FunctionRunner]].
 *
 */
// scalastyle:on line.size.limit
package object dsl {

}
