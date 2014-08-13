echo "Example: $0 <master ip> <master port> <split> <sum> <runseconds>"
ip=$1
port=$2
split=$3
sum=$4
runseconds=$5

echo java -cp "target/scala-2.10/gearpump-assembly-0.4-SNAPSHOT.jar" org.apache.gearpump.examples.wordcount.WordCount  -ip $ip -port $port -split $split -sum $sum -runseconds $runseconds
java -cp "target/scala-2.10/gearpump-assembly-0.4-SNAPSHOT.jar" org.apache.gearpump.examples.wordcount.WordCount  -ip $ip -port $port -split $split -sum $sum -runseconds $runseconds
