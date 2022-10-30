# !/bin/sh

for pid in $(ps -ef | grep cs4224i | grep Wholesale-YCQL | grep -v grep | awk '{print $2}'); do
    echo $pid
    kill -9 $pid
done

exit 0