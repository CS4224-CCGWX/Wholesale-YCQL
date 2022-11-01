nodes=5

# Kills on each machine.
for ((c=0; c<$nodes; c++)); do
    nodeID="xcnd"$((20+$c%$nodes))
    ssh $nodeID "cd Wholesale-YCQL && ./scripts/alter_run_tserver.sh ${nodeID}"
    echo "Set up tserver on machine ID=${nodeID}."
done

exit 0
