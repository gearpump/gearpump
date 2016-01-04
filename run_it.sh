#!/bin/sh

echo "" > it.log
for i in `seq 1 100`;
do
  echo "\$\$\$\$\$\$\$\$\$\$ Run Integration Test: $i time  ##########" |tee -a it.log
  date |tee -a it.log
  sbt it:test 2>&1 |tee -a it.log
  echo "\n\n\n\n\n"
done    
