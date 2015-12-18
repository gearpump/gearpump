### Put YARN configuration files under classpath
  Before calling "yarnclient launch", make sure you have put all YARN configuration 
  files under classpath. Typically, you can just copy all files under 
  $HADOOP_HOME/etc/hadoop from one of the YARN Cluster machine to  "conf/yarnconf" 
  of gearpump.

NOTE: The "conf/yarnconf" is only effecive when you run "yarnclient". When you 
run other commands like "gear", "conf/yarnconf" will not be addded into classpath.