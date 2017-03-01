# Kafka, Zookeeper and Kafka 7 proxy

#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

FROM spotify/kafka

ADD kafka-0.7.2.jar kafka-0.7.2.jar
ADD zkclient-0.1.jar zkclient-0.1.jar
ADD consumer.properties consumer.properties
ADD producer.properties producer.properties
ADD start_proxy.sh /start_proxy.sh
ADD kafkaproxy.conf /etc/supervisor/conf.d/kafkaproxy.conf

ENV LOG_RETENTION_HOURS 1

ADD https://archive.apache.org/dist/kafka/0.8.1/kafka_2.8.0-0.8.1.tgz /
RUN cd / && tar xzf kafka_2.8.0-0.8.1.tgz
ENV TAIL_KAFKA_HOME /kafka_2.8.0-0.8.1

CMD ["supervisord", "-n"]
