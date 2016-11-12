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
  -c      Clean before building
EOF
}
VERIFY=false
RUN_RAT=false
CLEAN=false
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
         c)
             CLEAN=true
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
  echo Verifying ${GEARPUMP_RELEASE_VERSION}-bin.tgz.asc 
  gpg --verify ${GEARPUMP_RELEASE_VERSION}-bin.tgz.asc
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

if [ $CLEAN = "true" ]; then
  dev-tools/build clean reset scrub
fi
dev-tools/build all
PACKED_ARCHIVE=output/target/gearpump-2.11-${RELEASE_VERSION}.tar.gz
if [ ! -f $PACKED_ARCHIVE ]; then
  echo "missing $PACKED_ARCHIVE"
  echo "You must run 'sbt assembly pack pack-archive' first"
  exit 1
fi
mkdir tmp
cd tmp
tar xzf ../$PACKED_ARCHIVE
cp ../NOTICE ../README.md ../CHANGELOG.md .
cp ../LICENSE.bin LICENSE
rsync -a ../tmp/ $GEARPUMP_RELEASE_VERSION
tar czf ../${GEARPUMP_RELEASE_VERSION}-bin.tgz $GEARPUMP_RELEASE_VERSION
echo Signing ../${GEARPUMP_RELEASE_VERSION}-bin.tgz
echo $GPG_PASSPHRASE | gpg --batch --default-key $GPG_KEY --passphrase-fd 0 --armour --output ../${GEARPUMP_RELEASE_VERSION}-bin.tgz.asc --detach-sig ../${GEARPUMP_RELEASE_VERSION}-bin.tgz
gpg --print-md MD5 ../${GEARPUMP_RELEASE_VERSION}-bin.tgz > ../${GEARPUMP_RELEASE_VERSION}-bin.tgz.md5
gpg --print-md SHA1 ../${GEARPUMP_RELEASE_VERSION}-bin.tgz > ../${GEARPUMP_RELEASE_VERSION}-bin.tgz.sha
cd ..
rm -rf tmp



