#!/bin/bash

readonly CLASSPATH_FILE='classpath.out'
readonly TARGET_CLASS='org.linqs.psl.distributed.bibliographicER.DBLPACM'

FETCH_COMMAND=''

function err() {
   echo "[$(date +'%Y-%m-%dT%H:%M:%S%z')]: $@" >&2
}

# Check for:
#  - maven
#  - java
function check_requirements() {
   type mvn > /dev/null 2> /dev/null
   if [[ "$?" -ne 0 ]]; then
      err 'maven required to build project'
      exit 12
   fi

   type java > /dev/null 2> /dev/null
   if [[ "$?" -ne 0 ]]; then
      err 'java required to run project'
      exit 13
   fi
}

function check_arguments() {
   if [[ $# -ne 2 ]]; then
      echo ""
      err '--runid=RUNID and SIMTYPE are required arguments'
      echo ""
      cat usage.txt
      echo ""
      exit 70
   fi
   if [[ $1 != --runid=* ]]; then
      echo ""
      err 'First argument must have the form --runid=RUNID'
      echo ""
      cat usage.txt
      echo ""
      exit 80
   fi
}

function compile() {
   mvn compile
   if [[ "$?" -ne 0 ]]; then
      err 'Failed to compile'
      exit 40
   fi
}

function buildClasspath() {
   if [ -e "${CLASSPATH_FILE}" ]; then
      echo "Classpath found cached, skipping classpath build."
      return
   fi

   mvn dependency:build-classpath -Dmdep.outputFile="${CLASSPATH_FILE}"
   if [[ "$?" -ne 0 ]]; then
      err 'Failed to build classpath'
      exit 50
   fi
}

function run() {
   echo "$@"
   java -cp ./target/classes:$(cat ${CLASSPATH_FILE}) ${TARGET_CLASS} "$@"
   if [[ "$?" -ne 0 ]]; then
      err 'Failed to run'
      exit 60
   fi
}

function main() {
   check_requirements
   check_arguments "$@"
   compile
   buildClasspath
   run --master $1 data Similarities.$2.combo-only same_targets.txt same_truth.txt
}

main "$@"
