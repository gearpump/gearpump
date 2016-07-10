#!/bin/bash
CURDIR=`pwd`
CURDIRNAME=`basename $CURDIR`

if [ "$CURDIRNAME" != "docs" ]; then
  echo "This script MUST be run under 'docs' directory. "
  exit 1
fi

help() {
  echo -e "Usage: \n\t$0 scala_version[2.11] build_api_doc[0|1]"
  echo -e "\tE.g. $0 2.11 1"
}

copy_dir() {
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

# check html link validity
# htmlproof has been renamed to htmlproofer in Dec 2015
HTMLPROOF="htmlproof"
HTML_IGNORE_HREF='--href-ignore "#/"'
if ! `which $HTMLPROOF >/dev/null`; then
  HTMLPROOF="htmlproofer"
  HTML_IGNORE_HREF="--allow-hash-href"
fi
echo "Checking generated HTMLs using $HTMLPROOF..."

$HTMLPROOF _site \
  --disable-external \
  $HTML_IGNORE_HREF \
  --url-ignore \#,api/scala/index.html,api/java/index.html,/download.html

# generate API doc
if [ "$BUILD_API" = 1 ]; then
  # Build Scaladoc for Java/Scala
  echo "Moving to project root and building API docs."
  echo "Running 'sbt clean assembly unidoc'; this may take a few minutes..."
  cd $CURDIR/..
  sbt clean assembly unidoc
  echo "Moving back into docs dir."
  cd $CURDIR

  echo "Removing old docs"
  rm -rf _site/api

  #copy unified ScalaDoc
  copy_dir "../target/scala-$SCALA_VERSION/unidoc"  "_site/api/scala"

  #copy unified java doc
  copy_dir "../target/javaunidoc" "_site/api/java"
fi
