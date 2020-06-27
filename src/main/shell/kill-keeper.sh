ps -ef | grep "io.qross.keeper.Protector" | grep -v grep | awk '{print $2}' | xargs kill -9;
ps -ef | grep "io.qross.keeper.Keeper" | grep -v grep | awk '{print $2}' | xargs kill -9;
ps -ef | grep "qross-keeper-start-" | grep -v grep | awk '{print $2}' | xargs kill -9;

