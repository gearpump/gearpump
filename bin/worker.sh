echo "Example: $0 <master ip> <port>"
ip=$1
port=$2

echo java -cp "target/scala-2.10/gearpump-assembly-0.4-SNAPSHOT.jar" org.apache.gears.cluster.Starter worker -ip $ip -port $port
java -cp "target/scala-2.10/gearpump-assembly-0.4-SNAPSHOT.jar" org.apache.gears.cluster.Starter worker -ip $ip -port $port
