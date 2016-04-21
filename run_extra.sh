#!/bin/bash
PYTHON_FILE=$1
INPUT1=$2
INPUT2=$3
OUTPUT1=$4
OUTPUT2=$5
LOCAL_OUTPUT1=$6
LOCAL_OUTPUT2=$7
NUM_EXECS=$8
if [ $# -eq 8 ]; then
    NAME=$8
else
    NAME="BV"
fi
HD=hadoop
SP=spark-submit

$HD fs -rm -r -skipTrash $OUTPUT1
$HD fs -rm -r -skipTrash $OUTPUT2
$SP --name "$NAME" --num-executors $NUM_EXECS $PYTHON_FILE $INPUT1 $INPUT2 $OUTPUT1 $OUTPUT2
rm -f $LOCAL_OUTPUT1
rm -f $LOCAL_OUTPUT2
$HD fs -cat $OUTPUT1/part* | sort > $LOCAL_OUTPUT1
$HD fs -cat $OUTPUT2/part* | sort > $LOCAL_OUTPUT2
