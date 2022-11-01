curr_node=$1
port="9042"

if [[ $curr_node == "xcnd20" ]]; then
    ip="192.168.48.239"
elif [[ $curr_node == "xcnd21" ]]; then
    ip="192.168.48.240"
elif [[ $curr_node == "xcnd22" ]]; then
    ip="192.168.48.241"
elif [[ $curr_node == "xcnd23" ]]; then
    ip="192.168.48.242"
elif [[ $curr_node == "xcnd24" ]]; then
    ip="192.168.48.243"
else
    echo "Unknown node name: $curr_node"
    exit -1
fi

if [[ ! -d benchmark ]]; then
    mkdir benchmark
fi
output_path="./log/summary.out"
err_path="./log/summary.err"

echo "Run summary"
java -jar target/Wholesale-YCQL-1.0.jar summary $ip $port
exit 0