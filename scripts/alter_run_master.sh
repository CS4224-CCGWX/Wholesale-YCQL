export ybb="/home/stuproj/cs4224i/yugabyte-2.14.2.0/bin"

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
echo "Launching master on $ip ($curr_node)"

# List of ports to modify: https://docs.yugabyte.com/preview/deploy/manual-deployment/verify-deployment/#default-ports-reference
# yb-master reference: https://docs.yugabyte.com/preview/reference/configuration/yb-master/#general-flags
master1="192.168.48.239"
master2="192.168.48.240"
master3="192.168.48.241"
rpc_port="11451"
web_port="11452"

diskDir="/mnt/ramdisk"
rm -rf $diskDir/yugabyte-data
mkdir $diskDir/yugabyte-data/

$ybb/yb-master \
--master_addresses "$master1:$rpc_port,$master2:$rpc_port,$master3:$rpc_port" \
--rpc_bind_addresses "$ip:$rpc_port" \
--webserver_port $web_port \
--fs_data_dirs "$diskDir/yugabyte-data" >& $diskDir/yugabyte-data/yb-master.out &