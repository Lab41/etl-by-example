#!/bin/sh
set -e

pushd `dirname $0`

message="usage: run.sh [input dir] [output dir] -debug (optional)"

main=org.lab41.sample.etl.mapreduce.Driver
input=$1
output=$2

if [[ "$1" == "" ]]; then
    echo $message
    echo "Input directory required" 1>&2
    exit 1
fi

if [[ "$2" == "" ]]; then
	echo $message
    echo "Output directory required" 1>&2
    exit 1
fi

if [[ "$3" == "-debug" ]]; then
	HADOOP_OPTS="-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=1044"  
fi


hadoop jar ../target/best-practices-etl-1.0-SNAPSHOT.jar "$main" "$1" "$2" 

popd