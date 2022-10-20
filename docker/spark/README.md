# Spark Cluster in Docker
This section contains instructions on using a spark standalone cluster with 1 master and 1-2 worker nodes.

**Spark version**: 3.2.1

**Scala version**: 2.13.x

## Work with the Spark shell interactively
Change directory to the `docker/spark` folder,

1. Start the spark cluster
```
docker-compose up -d

```

2. Connect to the master node container. Exit the container at any time by typing in ```exit``` at the command line
```
docker exec -it spark_spark-master_1  /bin/bash
```

3. Start the interactive shell
```
bin/spark-shell
```

4. View the container logs
```
docker logs <container name>
```
For more logging options see: https://docs.docker.com/engine/reference/commandline/logs/


5. Stop spark clusters
```
docker-compose down
```

### License
Copyright 2022, Edward Sumitra

Licensed under the MIT License.