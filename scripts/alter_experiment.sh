consistency_level=${1-'all'}

nodes=5

submit_job() {
  job_id=$1
  consistency_level=${2-'all'}
  echo "submit job: "$job_id" with consistency level: "$consistency_level

  node_id="xcnd"$((20+$job_id%$nodes))
  echo "the job will be submitted to node: "$node_id

  ssh $node_id "cd Wholesale-YCQL && ./scripts/alter_run.sh ${node_id} ${job_id} ${consistency_level}"
}

for ((c=0; c<5; c++))
do
  submit_job $c $consistency_level &
done
wait
exit 0