### System timestamp and Application timestamp

System timestamp denotes the time of backend cluster system. Application timestamp denotes the time at which message is generated. For example, for IoT edge device, the timestamp at which field sensor device creates a message is type of application timestamp, while the timestamp at which that message get received by the backend is type of system time.

### Master, and Worker

Gearpump follow master slave architecture. Every cluster contains one or more Master node, and several worker nodes. Worker node is responsible to manage local resources on single machine, and Master node is responsible to manage global resources of the whole cluster.

![Actor Hierarchy](../img/actor_hierarchy.png)

### Application

Application is what we want to parallel and run on the cluster. There are different application types, for example MapReduce application and streaming application are different application types. Gearpump natively supports Streaming Application types, it also contains several templates to help user to create custom application types, like distributedShell.

### AppMaster and Executor

In runtime, every application instance is represented by a single AppMaster and a list of Executors. AppMaster represents the command and controls center of the Application instance. It communicates with user, master, worker, and executor to get the job done. Each executor is a parallel unit for distributed application. Typically AppMaster and Executor will be started as JVM processes on worker nodes.

### Application Submission Flow

When user submits an application to Master, Master will first find an available worker to start the AppMaster. After AppMaster is started, AppMaster will request Master for more resources (worker) to start executors. The Executor now is only an empty container. After the executors are started, the AppMaster will then distribute real computation tasks to the executor and run them in parallel way.

To submit an application, a Gearpump client specifies a computation defined within a DAG and submits this to an active master. The SubmitApplication message is sent to the Master who then forwards this to an AppManager.

![Submit App](../img/submit.png)
Figure: User Submit Application

The AppManager locates an available worker and launches an AppMaster in a sub-process JVM of the worker. The AppMaster will then negotiate with the Master for Resource allocation in order to distribute the DAG as defined within the Application. The allocated workers will then launch Executors (new JVMs).

![Launch Executors and Tasks](../img/submit2.png)
Figure: Launch Executors and Tasks

### Streaming Topology, Processor, and Task

For streaming application type, each application contains a topology, which is a DAG (directed acyclic graph) to describe the data flow. Each node in the DAG is a processor. For example, for word count it contains two processors, Split and Sum. The Split processor splits a line to a list of words, and then the Sum processor summarize the frequency of each word.
An application is a DAG of processors. Each processor handles messages.

![DAG](../img/dag.png)
Figure: Processor DAG

### Streaming Task and Partitioner

For streaming application type, Task is the minimum unit of parallelism. In runtime, each Processor is paralleled to a list of tasks, with different tasks running in different executor. You can define Partitioner to denote the data shuffling rule between upstream processor tasks and downstream processor tasks.

![Data Shuffle](../img/shuffle.png)
Figure: Task Data Shuffling
