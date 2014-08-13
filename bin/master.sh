## Create Master on <master ip>:80, 
echo "Example: $0 <port>"
port=$1

echo java -cp "target/scala-2.10/gearpump-assembly-0.4-SNAPSHOT.jar" org.apache.gears.cluster.Starter master -port $port
java -cp "target/scala-2.10/gearpump-assembly-0.4-SNAPSHOT.jar" org.apache.gears.cluster.Starter master -port $port
