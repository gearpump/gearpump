#!/bin/bash

function help {
  echo "Usage: \n\t$0 scala_version[2.11] build_api_doc[0|1]"
  echo "\tE.g. $0 2.11 1"
}

if [ $# -ne 2 ]; then
  help
  exit 1
fi

export SCALA_VERSION=$1
export BUILD_API=$2

# generate _site documents
jekyll build

# generate API doc
ruby copy_api_dirs.rb

# check html link validality
htmlproof _site \
  --disable-external \
  --url-ignore \#,api/scala/index.html,api/java-api.html
