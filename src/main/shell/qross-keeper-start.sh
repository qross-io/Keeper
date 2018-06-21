#!/bin/sh
ps -fe | grep keeper-0.5.4 | grep -v grep
if [ $? -ne 0 ]
then
    echo "start qross keeper....."
    /srv/jdk1.8/bin/java -cp /home/qross/qross-keeper-0.5.4.jar io.qross.keeper.Recorder
    /srv/jdk1.8/bin/java -cp /home/qross/qross-keeper-0.5.4.jar io.qross.keeper.Protector --debug --cluster --properties /data/config/qinling/databases.properties
    #hadoop jar /home/qross/qross-keeper-0.5.3.jar io.qross.keeper.Protector /config/databases.properties
else
     echo "qross keeper is running....."
fi