# APACHE FLINK sample
## Environment setup
1. Download Apache Flink binary
2. To start Flink cluster:
```shell
$ $FLINK_HOME/bin/start-cluster.sh
```
Open localhost:8081 for the console UI
3. To run a job
```shell
$ $FLINK_HOME/bin/flink run job.jar
```
With job.jar the uber jar that has Flink Job
## Local development with maven
1. Set up the maven project with flink quickstart archetype
```shell
$ mvn archetype:generate                               \
  -DarchetypeGroupId=org.apache.flink              \
  -DarchetypeArtifactId=flink-quickstart-java      \
  -DarchetypeVersion=1.12.0
```
. Make sure the main class is included with the shaded plugin in pom.xml

# References
- [Project Configuration](https://ci.apache.org/projects/flink/flink-docs-stable/dev/project-configuration.html)
