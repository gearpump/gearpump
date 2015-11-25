# Licensed under the Apache License, Version 2.0
# See accompanying LICENSE file.

# The base image contains JRE8
FROM stanleyxu2005/minimal-jre8

# Create SUT home, files will be mounted at runtime.
ENV SUT_HOME=/opt/gearpump
WORKDIR $SUT_HOME

# Setup the entry point
ADD start.sh /opt/start
RUN chmod +x /opt/start

ENTRYPOINT ["/opt/start"]
