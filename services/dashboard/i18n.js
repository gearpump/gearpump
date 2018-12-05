/*
 * Licensed under the Apache License, Version 2.0 (the
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
angular.module('dashboard')
/**
 * You can override any values below in your application
 */
  .constant('i18n', {
    terminology: {
      master: 'Every cluster has one or more Master node. Master node is responsible to manage global resources of the cluster.',
      worker: 'Worker node is responsible to manage local resources on single machine',
      workerExecutor: 'Executor JVMs running on current worker node.',
      appClock: 'Application clock tracks minimum clock of all message timestamps. Message timestamp is immutable since birth. It denotes the moment when message is generated.',
      appExecutor: 'In runtime, every application instance is represented by a single AppMaster and a list of Executors. AppMaster represents the command and controls center of the Application instance. It communicates with user, master, worker, and executor to get the job done. Each executor is a parallel unit for distributed application. Typically AppMaster and Executor will be started as JVM processes on worker nodes.',
      processor: 'For streaming application type, each application contains a topology, which is a DAG (directed acyclic graph) to describe the data flow. Each node in the DAG is a processor.',
      task: 'For streaming application type, Task is the minimum unit of parallelism. In runtime, each Processor is paralleled to a list of tasks, with different tasks running in different executor.'
    }
  })
;
