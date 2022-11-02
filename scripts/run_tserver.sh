# Specify yugabyte binary path
export ybb="/temp/yugabyte-2.14.1.0/bin"

# Specify master IPs and mater RPC port
master1="192.168.48.239"
master2="192.168.48.240"
master3="192.168.48.241"
port="7100"
compression="LZ4"

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

echo "Launching tserver on $curr_node with address $ip"

# Specify disk directory
diskDir="/mnt/ramdisk"
if [[ ! -d $diskDir/yugabyte-data ]]; then
    mkdir $diskDir/yugabyte-data/
fi

# Launch
$ybb/yb-tserver \
--tserver_master_addrs $master1:$port,$master2:$port,$master3:$port \
--rpc_bind_addresses $ip \
--compression_type $compression \
--fs_data_dirs "$diskDir/yugabyte-data" >& $diskDir/yugabyte-data/yb-tserver.out &
