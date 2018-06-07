 1 #!/bin/sh
  2 ps -fe | grep keeper-0.5 | grep -v grep
  3 if [ $? -ne 0 ]
  4 then
  5     echo "start keeper....."
        /srv/jdk1.8/bin/java -cp /home/panda/qross-keeper-0.5.5.jar io.qross.keeper.Recorder
  6     hadoop jar /home/panda/qross-keeper-0.5.2.jar /data/config/qinling/databases.properties
  7 else
  8     echo "qross keeper is runing....."
  9 fi