# Wholesale-YCQL

## Cluster setup
### Pre-requisite
- `yugabyte-2.14.1.0` installed under `/temp`, if not installed, run `./scripts/install_yugabyte.sh`.
- Make sure `/mnt/ramdisk` has enough space (> 10G).
- Make sure yugabyte default ports are availalbe.

### Setup steps
#### Start cluster on `xcnd20 - xcnd24`
- On `xcnd20`, run `./scripts/start_cluster.sh`.
- Check if server is up by run `ps aux | grep -E 'master|tserver'` on all cluster nodes. Make sure the 3 masters are up, and 5 tservers are up.
#### Start cluster on alternative cluster nodes
- If setting up cluster on alternative nodes, please modify the node names and IP addresses in `run_master.sh`, `run_tserver.sh`, and `start_cluster.sh`.
- The other scripts involving specifying IPs and node names are also to be modified.

## Run YCQL Java application benchmark
### Pre-requisite
- `JDK 11`
- `Maven`
- `Python 3.7` with `Pandas` package (neccessary for data pre-processing).

### Build and package Java application
```
mvn clean -f "./pom.xml"
mvn package -f "./pom.xml"
```
Make sure the project jar file is generated at `./target/Wholesale-YCQL-1.0.jar`

### Run benchmark
- Run `./scripts/benchmark.sh` to start benchmarking.
- Check `./log` for each transaction files' output and errors.
