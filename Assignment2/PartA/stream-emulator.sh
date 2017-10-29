#!/bin/bash

SOURCE="/spark-streaming/source"
DEST="/spark-streaming/stream"
CMD="hdfs dfs -mv"
#CMD=mv
#i=`hdfs dfs -ls $SOURCE | grep '.csv' | sort -n | head -1 | cut -d '.' -f1`
i=`hdfs dfs -stat "%n"  $SOURCE/* | grep '.csv' | sort -n | head -1 | cut -d '.' -f1`
#i=1
while [ "$i" ]
do
    file=$i.csv
    echo "Moving file: "$file
    $CMD $SOURCE/$file $DEST/$file
    i=`hdfs dfs -stat "%n"  $SOURCE/* | grep '.csv' | sort -n | head -1 | cut -d '.' -f1`
    #i=`hdfs dfs -ls $SOURCE | grep '.csv' | sort -n | head -1 | cut -d '.' -f1`
    sleep 5
done
