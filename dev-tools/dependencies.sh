#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
# 
#   http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

#
# This will generate a file LICENSE.dependencies which can be used as input to the LICENSE.bin (binary release)
# Some additional work is needed to automate the LICENSE.bin file generation - and it may be difficult to do so.
# LICENSE.dependencies does save time by categorizing dependencies under the different licenses
#
sbt dependencyLicenseInfo | tee dependencyInfo
cat dependencyInfo | sed -E "s/"$'\E'"\[([0-9]{1,3}((;[0-9]{1,3})*)?)?[m|K]//g" | grep '^\[info\]' | grep -v '^\[info\] Updating'|grep -v '^\[info\] Resolving'|grep -v '^\[info\] Done'|grep -v '^\[info\] Loading '|grep -v '^\[info\] Set ' > licenses.out 
cat licenses.out | grep '\[info\] [A-Z]' | sed 's/^\[info\] //' | sort | uniq > license.types
# add a space after 'No license specified'
sed  -n '/^\[info\] No license specified$/ {
=
p
}' licenses.out | grep -v '^\[info\]' > lines
cat lines | sort -nr > lines1
mv lines1 lines
for i in $(<lines);do
echo ex -sc "'"${i}'i|[info] '"'" -cx licenses.out > cmd
sh cmd
done

rm -f LICENSE.dependencies
touch LICENSE.dependencies
cat license.types | while read LINE; do 
  echo cat licenses.out \| sed "'"'/^\[info\] '$LINE'$/,/^\[info\] $/!d;//d'"'" \| sort \| uniq > cmd
  echo "$LINE" >> LICENSE.dependencies
  sh cmd >> LICENSE.dependencies
  echo ' ' >> LICENSE.dependencies
done
cat LICENSE.dependencies | sed 's/^\[info\] //' > LICENSE.d
mv LICENSE.d LICENSE.dependencies

#cleanup
rm -f cmd license.types licenses.out lines dependencyInfo
