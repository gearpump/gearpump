/*
 * Licensed under the Apache License, Version 2.0
 * See accompanying LICENSE file.
 */

angular.module('dashboard')
/**
 * You can override any values below in your application
 */
  .constant('i18n', {
    terminology: {
      master: 'Every cluster has one or more Master node. Master node is responsible to manage global resources of the cluster.',
      worker: 'Worker node is responsible to manage local resources on single machine',
      workerExecutor: 'Executor JVMS running on current worker node.',
      appExecutor: 'In runtime, every application instance is represented by a single AppMaster and a list of Executors. AppMaster represents the command and controls center of the Application instance. It communicates with user, master, worker, and executor to get the job done. Each executor is a parallel unit for distributed application. Typically AppMaster and Executor will be started as JVM processes on worker nodes.',
      processor: 'For streaming application type, each application contains a topology, which is a DAG (directed acyclic graph) to describe the data flow. Each node in the DAG is a processor.',
      task: 'For streaming application type, Task is the minimum unit of parallelism. In runtime, each Processor is paralleled to a list of tasks, with different tasks running in different executor.'
    }
  })
;