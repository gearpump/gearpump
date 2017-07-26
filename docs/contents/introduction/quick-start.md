This quick start will walk you through executing your first Gearpump pipeline to run WordCount written in Stream DSL.

### Set up development environment
1. Download and install [Java Development Kit(JDK)](http://www.oracle.com/technetwork/java/javase/downloads/index.html) 1.8. Verify that [JAVA_HOME](https://docs.oracle.com/javase/8/docs/technotes/guides/troubleshoot/envvars001.html) is set and points to your JDK installation.
2. Download and install [sbt](http://www.scala-sbt.org/download.html) by following [sbt's installation guide](http://www.scala-sbt.org/0.13/docs/Setup.html) for your specific operating system.

### Run WordCount

1. Run WordCount example with sbt

    
		:::bash
		sbt "project gearpump-examples-wordcount" run

   
2. Select the third main class in the sbt console

		:::bash
		Multiple main classes detected, select one to run:
		[1] org.apache.gearpump.streaming.examples.wordcount.WordCount
		[2] org.apache.gearpump.streaming.examples.wordcount.dsl.WindowedWordCount
		[3] org.apache.gearpump.streaming.examples.wordcount.dsl.WordCount

		Enter number: 3

     If everything goes fine, the following output is expected

		:::bash
		(is,1)(bingo!!,2)(a,1)(good,1)(This,1)(start,,1)



   

