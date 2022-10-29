curr_node=$1
tx=$2
consistency_level=${3-'all'}
port="2333"

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
elif [[ $curr_node == "localhost" ]]; then
    ip="localhost"
else
    echo "Unknown node name: $curr_node"
    exit -1
fi

input_path="./project_files/xact_files/"$tx".txt"
if [[ ! -d log ]]; then
    mkdir log
fi
output_path="./log/"$tx".out"
err_path="./log/"$tx".err"

echo "Run transaction file "$input_path" at consistency_level: "$consistency_level" at node: "$curr_node
java -jar target/Wholesale-YCQL-1.0.jar run $ip $port $consistency_level < $input_path > $output_path 2> $err_path
exit 0