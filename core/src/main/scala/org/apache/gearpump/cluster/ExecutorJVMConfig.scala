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

package org.apache.gearpump.cluster

/**
 * classPath: When a worker create a executor, the parent worker's classpath will
 * be automatically inherited, the application jar will also be added to runtime
 * classpath automatically. Sometimes, you still want to add some extraclasspath,
 * you can do this by specify classPath option.
 *
 *
 */
case class ExecutorJVMConfig(classPath : Array[String], jvmArguments : Array[String], mainClass : String, arguments : Array[String], jar: Option[AppJar], username : String)