echo "Example: $0 <master ip> <port>"
ip=$1
port=$2

echo java -cp "lib/*" org.apache.gears.cluster.Starter worker -ip $ip -port $port
java -cp "lib/*" org.apache.gears.cluster.Starter worker -ip $ip -port $port