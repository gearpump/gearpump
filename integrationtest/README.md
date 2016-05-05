## Getting Started

To run the integration test, you need a Linux with Kernel version >= 3.10 and Docker 1.7 (or higher). The test framework will use several Docker images.
These docker image **NEED** to be prepared **BEFOREHAND** to avoid timeout during testing:

 * [The Gearpump Cluster Launcher and Storm Client](https://hub.docker.com/r/stanleyxu2005/gearpump-launcher/)
   `docker pull stanleyxu2005/gearpump-launcher`
 * [The standalone single node Kafka cluster with Zookeeper](https://hub.docker.com/r/spotify/kafka/)
   `docker pull spotify/kafka`
 * [The Hadoop image](https://hub.docker.com/r/sequenceiq/hadoop-docker/)
   `docker pull sequenceiq/hadoop-docker:2.6.0`

### Install Docker

The integration test framework use docker to simulate a real cluster. The test script assume the docker
 command can be executed without prefixing with `sudo`, and without asking password.

 In Docker site, there are instructions about how to configure docker to avoid using sudo, with command like
 this:  `sudo usermod -aG docker $(whoami)`

 Please refer to the Docker documents for more information.

 1. For CentOS, please check [How to create a docker group in CentOS](https://docs.docker.com/engine/installation/linux/centos/#create-a-docker-group)
 2. For Ubuntu, please check [How to create a docker group in Ubuntu](https://docs.docker.com/engine/installation/linux/ubuntulinux/#create-a-docker-group).

### Step to Run Tests

1. Checkout Gearpump project
   `git clone https://github.com/apache/incubator-gearpump.git`
2. Build Gearpump project
   `sbt assembly pack`
3. Run Integration test
   `sbt it:test`

The test will launch a Gearpump cluster with 1 master and 2 worker nodes as 3 Docker containers. It might take 10-20 minutes to go through all the test cases. It depends on, how powerful your machine is. The Docker container itself does not have a Gearpump distribution. It will link your local build to the Docker container. When tests are finished, you can see the test result on the screen, or you can save them to a file with this command `sbt it:test > test_report.out`. To investigate Gearpump log, please check the directory `output/target/pack/logs`.

### How To test single integration test suite like `io.gearpump.integrationtest.checklist.CommandlineSpec`?

Unfortunately, I searched around, and didn't find a clean way to do this in sbt. Gearpump is using nested suite for
integration test, which I think sbt don't support well with `sbt test-only <className>`. Please also see discussion at:
`https://groups.google.com/forum/#!topic/scalatest-users/l8FK7_I0agU`

For a not that clean solution, here is the steps:

1. Locate class `io.gearpump.integrationtest.suites.StandaloneModeSuite` source file at
  `gearpump/integrationtest/core/src/it/scala/io/gearpump/integrationtest/suites`
2. Document out suite you don't want to test like this:

  ```
    class StandaloneModeSuite extends Suites(
      new CommandLineSpec
    //  ,new ConnectorKafkaSpec,
    //  new RestServiceSpec,
    //  new ExampleSpec,
    //  new DynamicDagSpec,
    //  new StabilitySpec,
    //  new StormCompatibilitySpec,
    //  new MessageDeliverySpec
    )
  ```
3. Run `sbt it:test`

## Manual test by creating docker cluster manually

To launch a Gearpump cluster manually, you can run the commands as follows.
You can launch as many worker containers as you wish, but only one master for the time being.

```
export GEARPUMP_HOME=/path/to/gearpump/dist

## Start Master node
docker run -d -h master0 -v /etc/localtime:/etc/localtime:ro -e JAVA_OPTS=-Dgearpump.cluster.masters.0=master0:3000 -v $GEARPUMP_HOME:/opt/gearpump -v /tmp/gearpump:/var/log/gearpump --name master0 stanleyxu2005/gearpump-launcher master -ip master0 -port 3000

## Start Worker0 node
docker run -d -h worker0 -v /etc/localtime:/etc/localtime:ro -e JAVA_OPTS=-Dgearpump.cluster.masters.0=master0:3000 -v $GEARPUMP_HOME:/opt/gearpump -v /tmp/gearpump:/var/log/gearpump --link master0 --name worker0 stanleyxu2005/gearpump-launcher worker

## ...

## Start Worker1 node
docker run -d -h worker1 -v /etc/localtime:/etc/localtime:ro -e JAVA_OPTS=-Dgearpump.cluster.masters.0=master0:3000 -v $GEARPUMP_HOME:/opt/gearpump -v /tmp/gearpump:/var/log/gearpump --link master0 --name worker0 stanleyxu2005/gearpump-launcher worker

```

Have a nice test drive!