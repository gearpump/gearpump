echo "Example: $0 <master ip> <master port>"
ip=$1
port=$2

echo java -cp "lib/*" org.apache.gearpump.app.examples.wordcount.WordCount $ip $port
java -cp "lib/*" org.apache.gearpump.app.examples.wordcount.WordCount $ip $port