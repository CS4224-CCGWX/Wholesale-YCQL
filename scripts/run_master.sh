# Specify yugabyte binary path
export ybb="/temp/yugabyte-2.14.1.0/bin"

# Specify the master IPs and RPC port
master1="192.168.48.239"
master2="192.168.48.240"
master3="192.168.48.241"
port="7100"

# Get current node IP
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

echo "*** Launching master on $curr_node with address $ip:$port ***"

# Specify disk directory
diskDir="/mnt/ramdisk"
rm -rf $diskDir/yugabyte-data
mkdir $diskDir/yugabyte-data/

# Launch
$ybb/yb-master \
--master_addresses "$master1:$port,$master2:$port,$master3:$port" \
--rpc_bind_addresses "$ip:$port" \
--fs_data_dirs "$diskDir/yugabyte-data" >& $diskDir/yugabyte-data/yb-master.out &
