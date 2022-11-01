schema="/home/stuproj/cs4224i/Wholesale-YCQL/src/main/resources/schema0.ycql"
data="/home/stuproj/cs4224i/Wholesale-YCQL/project_files/data_files"

curr_node=$1

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

java -jar target/Wholesale-YCQL-1.0.jar load_data $ip $schema $data