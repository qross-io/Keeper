#!/bin/sh
ps -fe | grep keeper-0.5.4 | grep -v grep
if [ $? -ne 0 ]
then
    echo "start qross keeper....."
    hadoop jar /home/qross/qross-keeper-0.5.3.jar io.qross.keeper.Keeper /config/databases.properties
else
    echo "qross keeper is running....."
fi