#!/bin/sh
ps -fe | grep "io.qross.keeper.Keeper" | grep -v grep
if [ $? -ne 0 ]
then
    echo "start qross keeper....."
    ps -ef | grep "io.qross.keeper.Protector" | grep -v grep | awk '{print $2}' | xargs kill -9
    /srv/jdk1.8/bin/java -cp /home/panda/qross-keeper-0.5.4.jar io.qross.keeper.Recorder
    /srv/jdk1.8/bin/java -cp /home/panda/qross-keeper-0.5.4.jar io.qross.keeper.Protector --debug --cluster --properties /data/config/qinling/databases.properties
else
    echo "qross keeper is running....."
fi
