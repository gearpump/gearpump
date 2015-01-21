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
