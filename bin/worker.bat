echo "Example: %0 <master ip> <port>"
SET ip=%1
SET port=%2

java -cp "lib/*" org.apache.gears.cluster.Starter worker -ip %ip% -port %port%