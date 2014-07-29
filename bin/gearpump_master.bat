## Create Master on <master ip>:80, 
echo "Example: %0 <port>"
SET port=%1

java -cp "lib/*" org.apache.gears.cluster.Starter master -port %port%