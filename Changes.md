gearpump-0.3.0-rc1
====================
Change List:
---------------
 - #510, add log directory for UI server
 - #485, retain inactive application history in Master
 - #504, 1) AppMaster return more detailed application runtime information. 2) fix a worker bug when returning executors which run on worker. 
 - #422, [UI] show the number of executors (2) changed layout of application page
 - #417, add a experiment module to distribute a zip file to different machines
 - #502, fix KafkaStorage loading data for async kafka consumer
 - #422, [UI] (1) added home directory in UI (2) removed millis from duration 3) updated dag control height
 - #476, fix worker and master log path format in rest service
 - #498, worker rest data is not updated in UI
 - #397, move distributed shell from experiments to examples folder
 - #493 add implicit sender so that the Task can send itself messages[work around]
 - #427, use new kafka producer in kafka 0.8.2
 - #422, added cluster page to show master and worker information
 - #489, make the worker rest information easier to parse
 - #202, Add a default serializer for all kinds of messages
 - #477, REST Workers should return more information
 - #456, uses webjars to serve visjs 3.10.0
 - #483, upgrade visdag to 3.10 because widget does not repaint correctly when a node is moved
 - #473, Use webjars with spray support instead of bower to serve static resources
 - #477, return more data for rest call workers/
 - #474, fix rest service UT test fail.
 - #479, publish test jar as artifacts
 - #419, Reorder the application log by master startup timestamp
 - #456, Use visdag to render the graph DAG
 - #464， Travis bower integration
 - #394, fix kafka producer hang issue
 - #468, For test code, the ClockService will throw excepton when the DAG is not defined
 - #465, fix appname prefix bug
 - #461, AppMasterSpec and WorkServiceSpec UT are failing 
 - #270, Create a force-direct dag 
 - #453, Add rest service to serve master info 
 - #423, refactor task by seperating TaskActor and Task
 - #422, add worker rest api
 - #449: avoid load external resource by removing all CDN external links
 - #397 refactor distributed shell by using new Cluster API
 - #441, ui-portal is failed to build because of spray version conflict
 - #430 use application name as unique identifier for an application
 - #440, moved dashboard code from conf to service/dashboard
 - #402, refactor task manager
 - #280, Add websockets to REST api
 - #269, Define UI Dashboard structure
 - #406, custom executor jvm config in gear.conf is not effective.
 - #394, fix ActorSystemBooter process not shut down after application finishes
 - #412, add version.sbt 
 - #410 Add sbt-eclipse plugin and wiki for how to build gearpump on eclipse
 - #408, handle Ctrl+C(sigint) gracefully.
 - #396, bump up kafka version to 0.8.2.0
 - #398, Expose more metrics info
 - #389, fix kafka fetch thread bug 
 - #386 UT fail due to unclosed sequence file
 - #370, refactor streaming appmaster
 - #358, use uPickle for REST Service
 - #372, fix DAG subgraph build and more Graph UT
 - #377, fix KafkaSource.pull performance due to the List.append is not having good performance
 - #378, construct the serializer explicitly, instead of implicitly via the kryo serializer akka extension
 - #380, set the context class loader as the URL class loader for ActorSystem.


gearpump-0.2.3
====================
Change List:
---------------
 - #333, KafkaUtilSpec causes out of memory on travis
 - #335, #359， Enable auto-deployment to sonatype
 - #299, Some UT may fail randomly, most because of the expectMsg time out
 - #338, fix kafka leader not available exception
 - #349, scoverage dependencies get into snapshot build binaries.
 - #352, add RollingCountSpec
 - #356, User's application.conf can not be loaded due to empty config.
 - #373, add more restrict checks for warning, deprecation and feature

gearpump-0.2.2
====================
Change List:
-----------------
 - #327 fix 0.2.1 build error
 - #308 add another dag example project
 - #330 Allow user to output the metrics to log file besides graphite

gearpump-0.2.1
====================

Change List:
-----------------
 - #244, add more UT, 
 - #250, refactor kafka implementations
 - #301, fix UserConfig ClassNotFoundException
 - #306, ClassNotFound for customized Application type
 - #299 fix SeqFileStreamProcessorSpec
 - #320 fix dead lock in StreamTestUtil.createEchoTaskActor
 - #317, allow user to customize the akka system config for appmaste… 
 - #244, add KafkaUtilSpec and kafka integration test

gearpump-0.2
=========================

Highlights:
-----------------
 - Published a tech paper about the design on https://typesafe.com/blog/gearpump-real-time-streaming-engine-using-akka
 - UT coverage rate to 70%
 - Add support for general replay-able data source to support at least once delivery. 
 - More robust streaming. Be resilient to message loss, message duplication, and zombie processes. 
 - Refactor Kafka data source for at least once delivery.
 - Support general applications besides streaming, add an experimental distrubtedshell application under experiments/.
 - Re-defined the AppMaster, and Task interface, It is much easier to write an application now.
 - Support submitting and distributing large applications jars.
 - Add CI tool, nightly build, code coverage, and defined a formal commit guideline.

Change list:
-------------------
 - #274 AppMaster cannot connect to worker if there are multiple interface on one machine 
 - #272 Too many dead log mesages in the log
 - #266 Kafka grouper conf is incorrect 
 - #259 fix stream replay api and impl
 - #245 jacoco conflict with codecov
 - #244 Add more unit test for better test coverage
 - #242 Add application sumbittor username so that we can seperate the logs for different user
 - #239 REST AppMasterService test failed
 - #237 Add more information in log line
 - #233 Add TIMEOUT for interactive messages between two parties
 - #230 Executor fails to connect with AppMaster when in a multiple NIC environment
 - #229  Add a default cluster config under conf/
 - #226 return error message to user if not enough slots
 - #225 Gearpump logging application logs to different streams(and to persistent storage)
 - #222 TimeStampFilter implementation is wrong
 - #221 AppMastersDataRequest, AppMasterDataRequest and AppMasterDataDetailRequest should return information about failed appmaster attempts
 - #217 Write a small custom AppMaster example, which can run distributed shell.
 - #216 Support large application jars
 - #215 Improve the API and configs so it more easy to write and submit a new application.
 - #213 Add documents about how we do benchmark
 - #212 when network partition happen there maybe zombie tasks still sending messages for a while
 - #208 Support long-running applications
 - #196 When AppMasterStarter fails to load a class, the whole Gearpump cluster crash
 - #191 Add docs for all examples
 - #184 When packing example to a uber jar, should not include core and streaming jars
 - #176  Fix NPE for REST /appmaster/0?detail=true when no appmasters have launched
 - #169 Convert REST CustomSerializers to extend DefaultJsonProtocol
 - #162 version conflicts between dependencies of sub projects  bug
 - #148 Revert code to be Java 6 compatible
 - #145 Add instructions document for Gearpump commit process
 - #138 Update ReadMe.md project description
 - #135 Add travis build|passing status icon to README.md
 - #130 AppMaster, TaskActor class references should not be explicitly referenced in SubmitApplication and TaskDescription  - messages
 - #127 Document how to run coverage reports, fix sigar exception seen during tests.
 - #117 Fix style and code warnings, use slf4j where appropriate  
 - #116 Add unit tests to gearpump-core
 - #115 Setup travis CI and codecov.io for Gearpump
 - #112 Break up examples into separate projects as prerequisite to #100
 - #110 Netty Java code cleanup
 - #108 Spout and Bolt classes should be renamed
 - #106 Add unit tests for REST api  
 - #104 ActorInitializationException while running wordCount in local mode
 - #103 Build error, unable to resolve akka-data-replication
