#!/bin/bash

# Licensed under the Apache License, Version 2.0
# See accompanying LICENSE file.

# Root directory, put the Gearpump source code in this directory.
# The report directory and report.zip will be generated in this directory, after integration test.
rootDir="/root/integrationtest"

# The gearpump repo
gearpumpRepo="https://github.com/gearpump/gearpump.git"

# The mail recipients list file. One line one recipient.
# Please put this file in the same directory as this bash file.
mailList="maillist.txt"

# The subject of mail.
subject="Gearpump integration test failed"

# The message body of the mail
mailText="Gearpump integration test failed, please check the report for detail."

# Interval time (seconds) to check the code.
interval=60

# get the mail list
sendTo=""
while read line; do
  echo "${line}" | grep -q -E "^$|^#" && continue
  [ -z ${line} ] && continue
  line=$(echo ${line})
  sendTo=${sendTo}" "${line}
done < ${mailList}
sendTo=$(echo ${sendTo})

# clone the source code
cd ${rootDir}
rm -rf gearpump
git clone ${gearpumpRepo}
cd gearpump

# check and test
while true; do
  result=$(git pull origin master)
  if [[ ${result} == Updating* ]]; then
    sbt clean assembly packArchive
    rm -rf ${rootDir}/report*
    sbt "it:test-only *Suite* -- -h ${rootDir}/report"

    testResult=$?
    if [ ${testResult} -ne 0 ]; then
      zip -r ${rootDir}/report.zip ${rootDir}/report
      echo ${mailText} | mutt ${sendTo} -s "${subject}" -a ${rootDir}/report/index.html -a ${rootDir}/report.zip
    fi
  fi

  sleep ${interval}
done
