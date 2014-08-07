## Create Master on <master ip>:80, 
echo "Example: $0 <port>"
port=$1

echo java -cp "lib/*" org.apache.gears.cluster.Starter master -port $port
java -cp "conf:lib/*" org.apache.gears.cluster.Starter master -port $port
