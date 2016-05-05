## Getting Started

This package provides the facility to do continuous integration. Assume you already know how to run 
integration test manually. If you don't know, Please refer [the instruction](https://github.com/apache/incubator-gearpump/blob/master/integrationtest/README.md) to setup your environment first. The 
integration test framework requires Docker. 

To take a test drive, please run: 

    sbt clean assembly pack
    sbt "it:test-only *Suite* -- -h report"

Test results will be stored in `report` directory.

## Run Integration Test Automatically

The bash script `autorun.sh` will clone the Gearpump source code from GitHub and monitor new commits 
of the master branch periodically. Once new commit is detected, it will build the source code and 
run the integration tests. Test results (including build failures) will be sent to the recipients, 
that are defined in `recipients.txt`.

Note that we use `mutt` to send mails. Please install it and configure it properly. E.g. in CentOS, run:

    sudo yum install mutt

Have fun.