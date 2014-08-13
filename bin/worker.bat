echo "Example: %0 <master ip> <port>"
SET ip=%1
SET port=%2

java -cp "target/scala-2.10/gearpump-assembly-0.4-SNAPSHOT.jar" org.apache.gears.cluster.Starter worker -ip %ip% -port %port%
