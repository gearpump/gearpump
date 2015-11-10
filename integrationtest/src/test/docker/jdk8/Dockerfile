# Licensed under the Apache License, Version 2.0
# See accompanying LICENSE file.

# The base image contains JDK8
FROM errordeveloper/oracle-jre

# Add gearpump distribution
ADD gearpump /gearpump
RUN chmod +x /gearpump/bin/*

# Setup the entry point
ADD start.sh /opt/start
RUN chmod +x /opt/start
ENTRYPOINT ["/opt/start"]