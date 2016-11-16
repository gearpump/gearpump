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
usage() {
cat << EOF
usage: $0 options 
OPTIONS:
  -h      Show this message
  -k      GPG_KEY 
  -p      GPG_PASSPHRASE
  -r      RAT Tool
  -v      Verify signed release
EOF
}
VERIFY=false
RUN_RAT=false
RELEASE_VERSION=$(grep '^version' version.sbt|sed 's/^.*"\(.*\)"$/\1/')
GEARPUMP_RELEASE_VERSION=gearpump-${RELEASE_VERSION}-incubating
while getopts “hrvk:p:” OPTION
do
     case "${OPTION}" in
         h)
             usage
             exit 1
             ;;
         k)
             GPG_KEY=$OPTARG
             ;;
         p)
             GPG_PASSPHRASE=$OPTARG
             ;;
         r)
             RUN_RAT=true
             ;;
         v)
             VERIFY=true
             ;;
         ?)
             usage
             exit
             ;;
     esac
done
shift $((OPTIND-1))
if [ $VERIFY = "true" ]; then
  gpg --import KEYS 2>/dev/null
  echo Verifying ${GEARPUMP_RELEASE_VERSION}-src.tgz.asc 
  gpg --verify ${GEARPUMP_RELEASE_VERSION}-src.tgz.asc
  exit 0
fi
if [ $RUN_RAT = "true" ]; then
  java -jar ~/rat/trunk/apache-rat/target/apache-rat-0.12-SNAPSHOT.jar -A -f -E ./.rat-excludes -d . | grep '^== File' 
  exit 0
fi
if [ -z $GPG_KEY ]; then
  echo Missing -k option
  usage
  exit 1
fi
if [ -z $GPG_PASSPHRASE ]; then
  echo Missing -p option
  usage
  exit 1
fi

dev-tools/build clean reset scrub
echo .git > exclude-list
echo .DS_Store >> exclude-list
for i in $(ls licenses/*|grep -v LICENSE-jquery.txt|grep -v LICENSE-bootstrap.txt); do
  echo $i >> exclude-list
done
cat exclude-list
rsync -a --exclude-from exclude-list ../incubator-gearpump/ $GEARPUMP_RELEASE_VERSION
tar czf ${GEARPUMP_RELEASE_VERSION}-src.tgz $GEARPUMP_RELEASE_VERSION
echo Signing ${GEARPUMP_RELEASE_VERSION}-src.tgz
echo $GPG_PASSPHRASE | gpg --batch --default-key $GPG_KEY --passphrase-fd 0 --armour --output ${GEARPUMP_RELEASE_VERSION}-src.tgz.asc --detach-sig ${GEARPUMP_RELEASE_VERSION}-src.tgz
gpg --print-md MD5 ${GEARPUMP_RELEASE_VERSION}-src.tgz > ${GEARPUMP_RELEASE_VERSION}-src.tgz.md5
gpg --print-md SHA1 ${GEARPUMP_RELEASE_VERSION}-src.tgz > ${GEARPUMP_RELEASE_VERSION}-src.tgz.sha
rm -rf ${GEARPUMP_RELEASE_VERSION} exclude-list


