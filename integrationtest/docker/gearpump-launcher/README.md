# GearPump Launcher Docker Image

The image helps developer to setup/test a local [Gearpump](https://github.com/gearpump/gearpump.git) cluster quickly. The image is based on a minimal JRE8 environment with Python support. 

## Usage

Here are the commands to launch a cluster. You can launch so many worker containers, as you wish. But only one master for the time being.
```
export GEARPUMP_HOME=/path/to/gearpump

docker run -d \
 -h master0 --name master0 \
 -v $GEARPUMP_HOME:/opt/gearpump \
 -e JAVA_OPTS=-Dgearpump.cluster.masters.0=master0:3000 \
 -p 8090:8090 \
 stanleyxu2005/gearpump-launcher \
 master -ip master0 -port 3000

docker run -d \
 --link master0 \
 -v $GEARPUMP_HOME:/opt/gearpump \
 -e JAVA_OPTS=-Dgearpump.cluster.masters.0=master0:3000 \
 stanleyxu2005/gearpump-launcher \
 worker

docker exec master0 gear info
docker exec master0 gear app -jar /path/to/userapp.jar [mainclass] [args]
```
