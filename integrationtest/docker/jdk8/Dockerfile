# Licensed under the Apache License, Version 2.0
# See accompanying LICENSE file.

# The base image contains JDK8
FROM errordeveloper/oracle-jre

# Create SUT home, files will be mounted at runtime.
ENV SUT_HOME=/opt/gearpump
WORKDIR $SUT_HOME

# Setup the entry point
ADD start.sh /opt/start
RUN chmod +x /opt/start

ADD storm.sh /opt/storm
RUN chmod +x /opt/storm

ENTRYPOINT ["/opt/start"]
