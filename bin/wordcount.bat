echo "Example: %0 <master ip> <master port>"
SET ip=%1
SET port=%2

java -cp "lib/*" org.apache.gearpump.app.examples.wordcount.WordCount %ip% %port%