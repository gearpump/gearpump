echo "Example: %0 <master ip> <master port> <split> <sum> <runseconds>"
SET ip=%1
SET port=%2
SET split=%3
SET sum=%4
SET runseconds=%5

echo java -cp "target/scala-2.10/gearpump-assembly-0.4-SNAPSHOT.jar" org.apache.gearpump.examples.wordcount.WordCount -ip %ip% -port %port% -split %split% -sum %sum% -runseconds %runseconds%
java -cp "target/scala-2.10/gearpump-assembly-0.4-SNAPSHOT.jar" org.apache.gearpump.examples.wordcount.WordCount -ip %ip% -port %port% -split %split% -sum %sum% -runseconds %runseconds%
