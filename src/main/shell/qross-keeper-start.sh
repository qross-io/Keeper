#!/bin/sh
ps -fe | grep keeper-0.5 | grep -v grep
if [ $? -ne 0 ]
then
    echo "start qross keeper....."
    /srv/jdk1.8/bin/java -cp /home/qross/qross-keeper-0.5.3.jar io.qross.keeper.Recorder
    hadoop jar /home/qross/qross-keeper-0.5.3.jar /config/databases.properties
else
     echo "qross keeper is running....."
fi