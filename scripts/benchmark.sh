consistency_level=${1-'all'}

num_nodes=5
base_node=20  # base node is xcnd20

submit_job() {
  job_id=$1
  consistency_level=${2-'all'}
  echo "submit job: "$job_id" with consistency level: "$consistency_level

  node_id="xcnd"$(($base_node + $job_id % $num_nodes))
  echo "the job will be submitted to node: "$node_id

  ssh $node_id "cd Wholesale-YCQL && ./scripts/run_jar.sh ${node_id} ${job_id} ${consistency_level}"
}

./scripts/dump_data.sh "xcnd${base_node}"
if [[ ! -d benchmark ]]; then
    mkdir benchmark
fi

for ((c=0; c<20; c++))
do
  submit_job $c $consistency_level &
done
wait

# Generate summary
/home/stuproj/cs4224i/Wholesale-YCQL/scripts/summary.sh xcnd20
exit 0
