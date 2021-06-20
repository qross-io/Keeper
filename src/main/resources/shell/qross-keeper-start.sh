#!/bin/sh
day=$(date "+%Y%m%d")
ps -fe | grep "io.qross.keeper.Keeper" | grep -v grep
if [ $? -ne 0 ]
then
    #echo "restart qross keeper." >> "/qross/keeper/beats/${day}.log"
    #ps -ef | grep "io.qross.keeper.Protector" | grep -v grep | awk '{print $2}' | xargs kill -9
    #/srv/jdk1.8/bin/java -cp /home/panda/qross-keeper-0.5.4.jar io.qross.keeper.Protector --debug --cluster --properties /data/config/qinling/databases.properties
    #`date +%F`.log
    /usr/bin/java -cp /usr/qross/qross-keeper-1.5.0.jar io.qross.keeper.Protector
#else
    #echo "qross keeper is running." >> "/qross/keeper/beats/${day}.log" 2>&1
    #>> file1 2>> file2
fi
#/usr/bin/java -cp /usr/qross/qross-kopeeper-0.6.4.jar io.qross.keeper.Inspector >> "/qross/keeper/beats/${day}.log" 2>&1