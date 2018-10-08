# kafka-java-moments

This repository contains code to deploy an example stream transformation microservice (MS). It will introduce several concepts, such as [Apache Spark](https://spark.apache.org/)'s [Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html), stateful stream transformation and Kappa architecture.
The streaming engine is assumed to be [Apache Kafka](https://kafka.apache.org/) and all of the following instructions are geared towards a deployment on [OpenShift](https://www.openshift.com/).

The MS will read a Kafka topic, which we will call `input`, perform the online estimation of the mean and variance for the data seen so far, and write the current estimates to a topic named `output`.

## Setup

The first thing to setup is the Kafka cluster, which in OpenShift can be done simply by installing [strimzi](http://strimzi.io/) by running

```shell
$ oc create -f https://raw.githubusercontent.com/strimzi/strimzi-kafka-operator/0.1.0/kafka-inmemory/resources/openshift-template.yaml
$ oc new-app strimzi
```

This will install an ephemeral Kafka cluster on OpenShift (the deployment might take a minute).

Next we install the radanalytics.io tooling to help you deploy Apache Spark clusters. This is done by running:

```
oc create -f https://radanalytics.io/resources.yaml
```

### Emitter

You can use any method you prefer to generate data for Kafka, but if you need a ready-made solution you can use the emitter app available [here](https://github.com/bones-brigade/kafka-openshift-react-sparkline/tree/master/test/emitter). To install it, simply run

```
oc new-app centos/python-36-centos7~https://github.com/bones-brigade/kafka-openshift-react-sparkline \
  --context-dir test/emitter \
  -e KAFKA_BROKERS=kafka:9092 \
  -e KAFKA_TOPIC=input \
  --name=emitter
```

This will generate a stream of random uniform numbers, between 0 and 100.

### Microservice

To install the actual stream transformation microservice, simply run

```
oc new-app --template oshinko-java-spark-build-dc \
    -p APPLICATION_NAME=skeleton \
    -p GIT_URI=https://github.com/ruivieira/kafka-java-moments \
    -p APP_MAIN_CLASS=bonesbrigade.service.moments.Main \
    -p SPARK_OPTIONS='--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0 --conf spark.jars.ivy=/tmp/.ivy2' \
    -e KAFKA_BROKERS=kafka:9092 \
    -e KAFKA_IN_TOPIC=input \
    -e KAFKA_OUT_TOPIC=output
```

This will listen to the `input` topic and write the estimated mean and variance of the stream to the `output` topic, with the JSON format:

```json
{
	"mean" : 45.012,
	"variance": 10.23
}
```

### Listener

Since the microservice does not retain any data, acting as a pure stream transformer, logging or inspection must be done running a listener app. As previously, you can use whichever method you prefer, but if you need a ready-made listener application, you can use the one available [here](https://github.com/bones-brigade/kafka-openshift-python-listener). To run it, simply issue:

```
oc new-app centos/python-36-centos7~https://github.com/bones-brigade/kafka-openshift-python-listener.git \
  -e KAFKA_BROKERS=kafka:9092 \
  -e KAFKA_TOPIC=output \
  --name=listener
```

If everything is working, you should see something similar to:

```
[...]
INFO:root:received: {"mean":49.836917378331066, "variance":852.7412814121482}
INFO:root:received: {"mean":49.8396090534979, "variance":852.7239661023062}
INFO:root:received: {"mean":49.84046492491253, "variance":852.7899157682115}
INFO:root:received: {"mean":49.84284685796561, "variance":852.7573660391838}
INFO:root:received: {"mean":49.841336760925415, "variance":852.6795046398067}
[...]
```
