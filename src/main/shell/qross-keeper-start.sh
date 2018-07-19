#!/bin/sh
ps -fe | grep "io.qross.keeper.Keeper" | grep -v grep
if [ $? -ne 0 ]
then
    echo "start qross keeper....."
    #ps -ef | grep "io.qross.keeper.Protector" | grep -v grep | awk '{print $2}' | xargs kill -9
    #/srv/jdk1.8/bin/java -cp /home/panda/qross-keeper-0.5.4.jar io.qross.keeper.Protector --debug --cluster --properties /data/config/qinling/databases.properties
    #`date +%F`.log
    /srv/jdk1.8/bin/java -cp /home/panda/qross-keeper-0.5.6.jar io.qross.keeper.Protector
else
    echo "qross keeper is running....."
    /srv/jdk1.8/bin/java -cp /home/panda/qross-keeper-0.5.6.jar io.qross.keeper.Notifier
fi