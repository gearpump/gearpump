![Alt text](http://g.gravizo.com/g?
@startuml;
participant "YARN NM";
participant YarnApplicationMaster;
participant ResourceManagerClient;
participant "YARN RM";
activate YarnApplicationMaster;
YarnApplicationMaster -> YarnApplicationMaster: create NMClientAsync;
YarnApplicationMaster -> "YARN NM": start;
activate "YARN NM";
YarnApplicationMaster --> ResourceManagerClient: create actor;
activate ResourceManagerClient;
ResourceManagerClient -> ResourceManagerClient: create AMRMClientAsync;
ResourceManagerClient -> "YARN RM": connect;
activate "YARN RM";
ResourceManagerClient --> YarnApplicationMaster: RMConnected;
YarnApplicationMaster --> ResourceManagerClient: RegisterAMMessage;
ResourceManagerClient -> "YARN RM": registerApplicationMaster;
ResourceManagerClient --> YarnApplicationMaster: RegisterAppMasterResponse;
YarnApplicationMaster --> ResourceManagerClient: ContainerRequestMessage;
ResourceManagerClient -> "YARN RM": addContainerRequest;
"YARN RM" --> ResourceManagerClient: ContainersAllocated;
ResourceManagerClient --> YarnApplicationMaster: ContainersAllocated;
deactivate ResourceManagerClient;
YarnApplicationMaster -> "YARN NM": startContainerAsync;
"YARN NM" --> YarnApplicationMaster: ContainerStarted;
deactivate YarnApplicationMaster;
deactivate "YARN NM";
deactivate "YARN RM";
@enduml;
)

How to Start the Gearpump cluster on YARN
=======================================
1. Create HDFS folder /user/gearpump/, make sure all read-write rights are granted.
2. Upload the gearpump-${version}.tar.gz jars to HDFS folder: /user/gearpump/
3. Modify the config file ```conf/yarn.conf.template``` or create your own config file
4. Upload the config file to HDFS folder: /user/gearpump/conf, and the file name should be ```gearpump_on_yarn.conf```
5. Start the gearpump yarn cluster, for example 
  ``` bash
  bin/yarnclient -version gearpump-$VERSION -config conf/yarn.conf
  ```

