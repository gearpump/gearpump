## Getting Started

To run the integration test, you need a Linux with Kernel version >= 3.10 and Docker 1.7 (or higher). The test framework will use two Docker images. They will be downloaded at the first time, when you launching the tests. You can also prepare them manually:

 * [The Gearpump Cluster Launcher and Storm Client](https://hub.docker.com/r/stanleyxu2005/gearpump-launcher)
   `docker pull stanleyxu2005/gearpump-launcher`
 * [The standalone single node Kafka cluster with Zookeeper](https://hub.docker.com/r/stanleyxu2005/kafka)
   `docker pull stanleyxu2005/kafka`

### Install Docker

The integration test framework is majorly developed on CentOS 7.0. We assume the command `docker` has added to `sudo` group without asking password. `sudo usermod -aG docker $(whoami)` Here is the [installation guide of Docker for CentOS](http://docs.docker.com/engine/installation/centos/). You can find more installation guide for other Linux/Mac OS distributions. If your machine is behind a firewall, here are [some leads](https://github.com/gearpump/gearpump-docker) to make it right.

### Step to Run Tests

1. Checkout Gearpump project
   `git clone https://github.com/gearpump/gearpump.git`
2. Build Gearpump project
   `sbt assembly pack`
3. Run Integration test
   `sbt it:test`

The test will launch a Gearpump cluster with 1 master and 2 worker nodes as 3 Docker containers. It might take 10-20 minutes to go through all the test cases. It depends on, how powerful your machine is. The Docker container itself does not have a Gearpump distribution. It will link your local build to the Docker container. When tests are finished, you can see the test result on the screen, or you can save them to a file with this command `sbt it:test > test_report.out`. To investigate Gearpump log, please check the directory `output/target/pack/logs`.

## Manual Test

To launch a Gearpump cluster manually, you can run the commands as follows. You can launch so many worker containers, as you wish. But only one master for the time being.
```
export GEARPUMP_HOME=/path/to/gearpump/dist

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
```

Have a nice test drive!
