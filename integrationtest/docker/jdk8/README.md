## Getting Started

Need Linux Kernel 3.10 (or higher) and Docker 1.7 (or higher). 

1. Install docker ([see also guide](https://github.com/gearpump/gearpump-docker))
2. Pull Docker image once `docker pull stanleyxu2005/gpct-jdk8`
3. Run the test suites

## Manual Test

To launch a Gearpump cluster manually, you can run the commands as follows. You can launch so many worker containers, as you wish. But only one master for the time being.
```
export GEARPUMP_HOME=/path/to/gearpump/dist

docker run -d \
 -h master0 --name master0 \
 -v $GEARPUMP_HOME:/opt/gearpump \
 -e CLUSTER=-Dgearpump.cluster.masters.0=master0:3000 \
 -p 8090:8090 \
 stanleyxu2005/gpct-jdk8 \
 master -ip master0 -port 3000

docker run -d \
 --link master0 \
 -v $GEARPUMP_HOME:/opt/gearpump \
 -e CLUSTER=-Dgearpump.cluster.masters.0=master0:3000 \
 stanleyxu2005/gpct-jdk8 \
 worker
```
