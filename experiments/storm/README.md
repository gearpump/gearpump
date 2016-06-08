The experimental storm module aims to provide binary compatibility for Storm applications over Gearpump. That is to say, users could easily grab an existing Storm jar and run it 
on Gearpump. This documentation illustrates how to do so in a local Gearpump cluster.

## How to run a Storm application over Gearpump 

1. launch a local cluster
  
   ```
   bin/local
   ```

2. start a Gearpump Nimbus server 

   Users need server's address(`nimbus.host` and `nimbus.thrift.port`) to submit topologies later. The address is written to a yaml config file set with `-output` option. 
   Users can provide an existing config file where only the address will be overwritten. If not provided, a new file `app.yaml` is created with the config.

   ```
   bin/storm nimbus -output [conf <custom yaml config>]
   ```
   
3. submit Storm applications
  
   Users can either submit Storm applications through command line or UI. 
   
   a. submit Storm applications through command line

     ```
     bin/storm app -verbose -config app.yaml -jar storm-starter-${STORM_VERSION}.jar storm.starter.ExclamationTopology exclamation 
     ```
  
     Users are able to configure their applications through following options
   
     * `jar` - set the path of a Storm application jar
     * `config` - submit the custom configuration file generated when launching Nimbus
  
   b. submit Storm application through UI
   
     1. Click on the "Create" button on the applications page on UI. 
     2. Click on the "Submit Storm Application" item in the pull down menu.
     3. In the popup console, upload the Storm application jar and the configuration file generated when launching Nimbus,
         and fill in `storm.starter.ExclamationTopology exclamation` as arguments.
     4. Click on the "Submit" button   

   Either way, check the dashboard and you should see data flowing through your topology. 

## Limitations 

1. Trident support is ongoing. 

