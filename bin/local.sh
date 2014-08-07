#!/bin/sh

echo "Example: $0 -port <port> -sameprocess -workernum <4>"

port=-1
sameprocess="false"
workernum=1

while test $# -gt 0
do
    case $1 in
        -port)
            port=$2
            shift
            ;;
        -sameprocess)
            sameprocess="true"
            ;;
        -workernum)
            workernum=$2
            shift
            ;;
        *)
            echo >&2 "Invalid argument: $1"
            ;;
    esac
    shift
done

echo java -cp "lib/*" org.apache.gears.cluster.Starter local -port $port -sameprocess $sameprocess -workernum $workernum
java -cp "conf:lib/*" org.apache.gears.cluster.Starter local -port $port -sameprocess $sameprocess -workernum $workernum
