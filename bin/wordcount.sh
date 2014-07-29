echo "Example: $0 <master ip> <master port> <split> <sum> <runseconds>"
ip=$1
port=$2
split=$3
sum=$4
runseconds=$5

echo java -cp "lib/*" org.apache.gearpump.app.examples.wordcount.WordCount  -ip $ip -port $port -split $split -sum $sum -runseconds $runseconds
java -cp "lib/*" org.apache.gearpump.app.examples.wordcount.WordCount  -ip $ip -port $port -split $split -sum $sum -runseconds $runseconds