# !/bin/sh

for pid in $(ps -ef | grep -E 'master|tserver' | grep -v grep | awk '{print $2}'); do
    echo $pid
    kill -9 $pid
done

exit 0