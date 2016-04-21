#!/bin/bash
PYTHON_FILE=$1
INPUT1=$2
INPUT2=$3
OUTPUT=$4
LOCAL_OUTPUT=$5
NUM_EXECS=$6
if [ $# -eq 7 ]; then
    NAME=$7
else
    NAME="BV"
fi
HD=hadoop
SP=spark-submit

$HD fs -rm -r -skipTrash $OUTPUT
$SP --name "$NAME" --num-executors $NUM_EXECS $PYTHON_FILE $INPUT1 $INPUT2 $OUTPUT
rm -f $LOCAL_OUTPUT
$HD fs -cat $OUTPUT/part* | sort > $LOCAL_OUTPUT

