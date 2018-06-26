#!/bin/sh
ps -fe | grep keeper-0.5.5 | grep -v grep
if [ $? -ne 0 ]
then
    echo "start qross keeper....."
    hadoop jar /home/qross/qross-keeper-0.5.5.jar io.qross.keeper.Keeper
else
    echo "qross keeper is running....."
    /srv/jdk1.8/bin/java -cp /home/panda/qross-keeper-0.5.5.jar io.qross.keeper.Notifier
fi