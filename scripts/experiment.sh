consistency_level=${1-'all'}

nodes=5

submit_job() {
  job_id=$1
  consistency_level=${2-'all'}
  echo "submit job: "$job_id" with consistency level: "$consistency_level

  node_id="xcnd"$((20+$job_id%$nodes))
  echo "the job will be submitted to node: "$node_id

  ssh $node_id "cd Wholesale-YCQL && ./scripts/run.sh ${node_id} ${job_id} ${consistency_level}"
}

./scripts/dump_data.sh xcnd20
if [[ ! -d experiment ]]; then
    mkdir experiment
fi

for ((c=0; c<20; c++))
do
  submit_job $c $consistency_level &
done
wait
exit 0