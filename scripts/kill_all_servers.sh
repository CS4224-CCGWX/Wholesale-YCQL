nodes=5

# Kills on each machine.
for ((c=0; c<$nodes; c++)); do
    nodeID="xcnd"$((20+$c%$nodes))
    ssh $nodeID "cd Wholesale-YCQL && ./scripts/kill_one_server.sh"
    echo "Have killed servers on machine ID=${nodeID}."
done

exit 0