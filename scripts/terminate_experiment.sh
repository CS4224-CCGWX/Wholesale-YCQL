nodes=3

# Kills on each machine.
for ((c=0; c<$nodes; c++)); do
    nodeID="xcnd"$((20+$c%$nodes))
    ssh $nodeID "ps aux | grep -ie Wholesale | grep -v grep | awk '{print $2}' | xargs kill -9"
    echo "Have killed on machine ID=${nodeID}."
done

exit 0