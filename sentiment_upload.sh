#!/bin/bash
export SENTIMENT_HOME=/root/sentiment_upload
export JAVA_HOME=/export/server/jdk1.8.0_151
export JAVA_CMD="${JAVA_HOME}/bin/java"
export JAVA_OPS="-jar ${SENTIMENT_HOME}/example-hdfs-1.0-SNAPSHOT-jar-with-dependencies.jar"

SOURCE_DIR=$1
PENDING_DIR=$2
OUTPUT_DIR=$3

if [ ! $SOURCE_DIR ] || [ ! $OUTPUT_DIR ]; then
    ${JAVA_CMD} ${JAVA_OPS} -h
    exit;
fi

if [ ! $PENDING_DIR ] ; then
    ${JAVA_CMD} ${JAVA_OPS} -s $SOURCE_DIR -o $OUTPUT_DIR
    exit;
fi

${JAVA_CMD} ${JAVA_OPS} -s $SOURCE_DIR -p ${PENDING_DIR} -o $OUTPUT_DIR
