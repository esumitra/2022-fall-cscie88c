# Docker based scala  environment

In project root,

1. Pull docker image
```
docker pull hseeberger/scala-sbt:eclipse-temurin-11.0.14.1_1.6.2_2.13.8
```

2. Run a terminal in docker
```
docker run -v `pwd`:/opt/projects/2022-fall-cscie88c -it --rm hseeberger/scala-sbt:eclipse-temurin-11.0.14.1_1.6.2_2.13.8 /bin/bash
```

3. cd to the mounted volume

```
cd /opt/projects/2022-fall-cscie88c
```

4. Run Scala application
```
sbt "run Harvard"

sbt "runMain org.cscie88c.week10.SparkDSApplication"
```

5. Exit shell
```
exit
```
