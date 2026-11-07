# TESTING.md — running isolated CT482 clusters (for lab porters / testers)

This host's Docker daemon is shared with other work. It already runs containers named
`spark-master`, `spark-worker-1`, `pyspark-notebook` holding host ports 8080, 7077, 8081, 8888, 4040-4041.
**Never stop, restart, remove or exec into them, and never prune.** Test clusters must use:

1. a **unique project name** (`-p ct482t1`, `-p ct482t2`, `-p ct482t3`): network, volumes and
   containers are then all scoped to that project (`ct482t1_default`, `ct482t1_namenode`, ...);
2. a **unique `CT_PREFIX`** (`t1-`, `t2-`, `t3-`): container names become `t1-namenode`, `t1-spark-master`, ...
   (hostnames inside the network stay `namenode`, `spark-master`, ... so lab commands and URLs are unchanged);
3. the **no-ports overrides** (`docker-compose.noports.yml`, `docker-compose.extras.noports.yml`):
   no host port is bound, so 3 clusters can run in parallel.

The image `bigdata-lab:2026.10` is already built on this host — **do not rebuild** unless you changed
`Dockerfile`, `conf/` or `scripts/` (and then tell the other testers, since the tag is shared).

## Start (tester N = 1, 2, 3)

```bash
cd moi-truong-bigdata-lab
export CT_PREFIX=t1-
export DC="docker compose -p ct482t1 -f docker-compose.yml -f docker-compose.extras.yml \
           -f docker-compose.noports.yml -f docker-compose.extras.noports.yml"

$DC up -d --wait --wait-timeout 600            # all 14 containers; ~30 s from scratch
# core only:
docker compose -p ct482t1 -f docker-compose.yml -f docker-compose.noports.yml up -d --wait
# a subset of extras (dependencies are pulled in automatically):
$DC up -d --wait hiveserver2
$DC ps
```

`CT_PREFIX` must be exported (or given inline) for **every** compose command of that project,
including `down`, otherwise compose looks for containers with the wrong names.

## Exec into containers

Use the prefixed name with plain `docker exec`, or the *service* name with `$DC exec`:

```bash
docker exec t1-namenode hdfs dfsadmin -report
docker exec t1-resourcemanager yarn node -list
docker exec t1-spark-master curl -s localhost:8080/json/          # Spark master JSON (aliveworkers)
docker exec t1-spark-master bash -lc 'spark-submit --master yarn --class org.apache.spark.examples.SparkPi $SPARK_EXAMPLES_JAR 10'
$DC exec namenode hdfs dfs -ls /
```

When a lab says `docker exec -it namenode ...`, run `docker exec -it t1-namenode ...`.
Use `bash -lc '...'` when the command references image variables (`$STREAM_JAR`, `$HADOOP_EXAMPLES_JAR`, ...).

### Files
`./data` → `/data` and `./src` → `/src` are bind mounts **shared by all test projects**
(same directory). Put per-tester files in sub-folders (`src/t1/...`, `data/t1/...`) to avoid clobbering each other.
`gen_data.py` writes to `./data`; generate once and share.

### Web UIs without host ports
```bash
docker exec t1-spark-master curl -s http://resourcemanager:8088/ws/v1/cluster/apps | jq '.apps.app[] | {id,name,state,finalStatus}'
docker exec t1-historyserver curl -s localhost:18080/api/v1/applications | jq '.[].name'
docker exec t1-historyserver curl -s localhost:19888/ws/v1/history/mapreduce/jobs | jq '.jobs.job[] | {id,state}'
```

### Non-interactive gotchas found during verification
- `beeline` needs a TTY. In scripts use stdin + dumb terminal (Hive 4.2.1 `beeline -f` stopped after the
  first statement in our runs):
  `docker exec -i -e HADOOP_CLIENT_OPTS=-Dorg.jline.terminal.provider=dumb t1-hiveserver2 beeline -u jdbc:hive2://localhost:10000/default -n hive --silent=true < script.sql`
  or `-e "stmt1; stmt2;"`.
- `hbase shell` from a script: `docker exec -i t1-hbase hbase shell -n < cmds.hbase` (or `hbase shell -n /src/file.hbase`).
- Kafka from the host (`localhost:9092`) cannot be tested in no-ports mode. Inside the network use `kafka:9092`.
  The EXTERNAL listener can be checked with a side-car that shares kafka's network namespace:
  `docker run --rm --network container:t1-kafka --entrypoint python3 bigdata-lab:2026.10 -c "from kafka import KafkaProducer; KafkaProducer(bootstrap_servers='localhost:9094').send('t', b'x').get(10)"`
- Do not test host-port behaviour (`localhost:8080` etc.) here — port 8080/7077/4040 belong to the user's own containers.

## Smoke test
`scripts-host/smoke_test.sh` runs the same checks used to verify the environment (HDFS, YARN, MR wordcount,
Hadoop Streaming, SparkPi standalone + YARN, PySpark parquet, and — if running — Hive, Kafka, HBase, MongoDB):

```bash
CT_PREFIX=t1- ./scripts-host/smoke_test.sh
```

## Teardown (always)

```bash
export CT_PREFIX=t1-
docker compose -p ct482t1 -f docker-compose.yml -f docker-compose.extras.yml \
  -f docker-compose.noports.yml -f docker-compose.extras.noports.yml down -v
# verify nothing of yours is left:
docker ps -a --filter name=^t1- ; docker volume ls --filter name=ct482t1 ; docker network ls --filter name=ct482t1
```

`down -v` removes only that project's containers, network and named volumes. Keep the images
(`bigdata-lab:2026.10`, `apache/hive:4.2.1`, `apache/kafka:4.3.1`, `postgres:16`, `mongo:8.0`).
Never use `docker system/image/volume prune` or `docker rm -f $(docker ps -aq)` on this host.

## Resource budget for 3 parallel clusters
Measured per cluster: core idle ≈ 2.3 GiB, core peak under MR+Spark ≈ 4.7 GiB, core + all extras idle ≈ 4.8 GiB.
Three full clusters ≈ 15 GiB idle, < 25 GiB under load — fine on this 61 GiB / 28-core host.
