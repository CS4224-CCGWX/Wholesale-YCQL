nodes=3

# Kills on each machine.
for ((c=0; c<$nodes; c++)); do
    nodeID="xcnd"$((20+$c%$nodes))
    #ssh $nodeID "cd Wholesale-YCQL && ./scripts/run_master.sh ${nodeID}"
    ssh $nodeID "cd Wholesale-YCQL && ./scripts/alter_run_master.sh ${nodeID}"
    echo "Set up master on machine ID=${nodeID}."
done

exit 0