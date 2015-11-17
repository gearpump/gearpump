#!/bin/bash
CURDIR=`pwd`
CURDIRNAME=`basename $CURDIR`
if [ "$CURDIRNAME" != "docs" ]; then
  echo "This script MUST be run under 'docs' directory. "
  exit 1
fi

function help {
  echo "Usage: \n\t$0 scala_version[2.11] build_api_doc[0|1]"
  echo "\tE.g. $0 2.11 1"
}

function copy_dir {
  srcDir=$1
  destDir=$2

  echo "Making directory $destDir"
  mkdir -p $destDir
  echo "copy from $srcDir to $destDir..."
  cp -r $srcDir/. $destDir
}

if [ $# -ne 2 ]; then
  help
  exit 1
fi

export SCALA_VERSION=$1
export BUILD_API=$2

# generate _site documents
jekyll build

# check html link validality
echo "Checking generated HTMLs..."
htmlproof _site \
  --disable-external \
  --url-ignore \#,api/scala/index.html,api/java/index.html

# generate API doc
if [ "$BUILD_API" = 1 ]; then
  # Build Scaladoc for Java/Scala
  echo "Moving to project root and building API docs."
  echo "Running 'sbt clean unidoc'; this may take a few minutes..."
  cd $CURDIR/..
  sbt clean unidoc

  echo "Moving back into docs dir."
  cd $CURDIR

  echo "Removing old docs"
  rm -rf _site/api

  #copy unified ScalaDoc
  copy_dir "../target/scala-$SCALA_VERSION/unidoc"  "_site/api/scala"

  #copy unified java doc
  copy_dir "../target/javaunidoc" "_site/api/java"
fi
