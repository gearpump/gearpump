#!/bin/bash

# Licensed under the Apache License, Version 2.0
# See accompanying LICENSE file.

RECIPIENTS_FILE="recipients.txt"
CHECK_INTERVAL="1m"
GIT_REPO="https://github.com/gearpump/gearpump.git"

# Read recipients from file
if [ ! -f $RECIPIENTS_FILE ]; then
  echo "Please specify recipients into \"$RECIPIENTS_FILE\" first."
  exit 1
fi
RECIPIENTS=$(grep -E "^$|^#" -v $RECIPIENTS_FILE | tr '\n' ';')

# Function to send mail
function mail_to {
  SUBJECT=$1; shift
  BODY=$1; shift
  echo -n "Sending mail ... "
  echo -e "$BODY" | mutt "$RECIPIENTS" -s "$SUBJECT" "$@"
  echo "done"
}

# Clone source code and go
BUILD_DIR=$(pwd)/gearpump
if [[ ! -d $BUILD_DIR ]]; then
  echo "Clone the project in $BUILD_DIR ..."
  git clone $GIT_REPO "$BUILD_DIR"
fi
cd "$BUILD_DIR" || exit 1

# Define the variables
DIST_DIR=output/target/pack
REPORT_DIR=report
COMMIT_REV=''

# Function to check new commit and run tests
function run_test_if_new_commit_found {
  echo -n "Check new Git commit ... "
  git pull -q origin master
  NEW_COMMIT_REV=$(git log -n 1 --pretty=format:'%h')
  if [[ $NEW_COMMIT_REV == "$COMMIT_REV" ]]; then
    echo "none"
    return
  fi

  echo "$NEW_COMMIT_REV detected"
  COMMIT_REV=$NEW_COMMIT_REV
  COMMIT_LOG=$(git log -1)

  rm -rf $DIST_DIR
  echo "Rebuild the project ..."
  sbt clean assembly packArchiveZip
  if [ $? -ne 0 ]; then
    mail_to \
      "Gearpump build failed $COMMIT_REV" \
      "Failed to build source code.\n\n$COMMIT_LOG"
    return
  fi

  rm -rf $REPORT_DIR
  echo "Run tests ... (it will take couple of minutes or longer)"
  sbt "it:test-only *Suite* -- -h $REPORT_DIR" | tee console.log
  if [ $? -ne 0 ]; then
    rm -f report.zip logs.zip
    zip -q -r report.zip $REPORT_DIR
    zip -q -r logs.zip $DIST_DIR/logs console.log
    mail_to \
      "Gearpump test failed $COMMIT_REV" \
      "Integration test failed. Please check attached reports and logs.\n\n$COMMIT_LOG" \
      "-a report.zip -a logs.zip"
  else
    mail_to \
      "Gearpump test passed $COMMIT_REV" \
      "All tests passed.\n\n$COMMIT_LOG"
  fi
}

while true; do
  run_test_if_new_commit_found
  echo -n "Sleep $CHECK_INTERVAL ... "
  sleep $CHECK_INTERVAL
done
