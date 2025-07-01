# cp-flink-workshop
Getting started hands-on with Confluent Platform for Apache Flink ("cp-flink")

This workshop provides a hands-on introduction to deploying and utilizing Confluent Platform for Apache Flink ("cp-flink") on a local Kubernetes cluster using Kind. Participants will learn to create Kubernetes instances for Kafka, Confluent Control Center, and Confluent Platform for Apache Flink. The session will then explore the Flink UI within Confluent Control Center for submitting and monitoring Flink applications. Finally, users will leverage the Confluent CLI to craft and execute Flink SQL statements, culminating in the creation of a data pipeline that reads from one Kafka topic, processes the data, and writes the results to another.

##Exercise 0: Install Confluent Control Center and CP-Flink

The exercise uses `kind` for a lightweight Kubernetes environment. As of late June 2025, we need Kubernetes 1.32.0 due to a known issue with included libraries. This will likely be resolved soon. Nonetheless, start `kind` like so:

```
kind create cluster --image kindest/node:v1.32.0
```
###Part 1: Confluent Server (Kafka), Confluent for Kubernetes (CFK), Confluent Control Center (C3)

Add the Confluent repository and install *Confluent for Kubernetes*:
```
helm repo add confluentinc https://packages.confluent.io/helm
helm repo update
helm upgrade --install operator confluentinc/confluent-for-kubernetes --version 0.1263.8 --namespace confluent --create-namespace
```
Exercise 0 uses Next Gen Confluent Control Center with the Flink CMF UI Enabled. The file [exercise-0/c3ng.yaml](exercise-0/c3ng.yaml) specifies this setup. View the file and note a couple of things:
1. We're using Confluent Control Center (next gen) 2.2.0, Confluent Platform 8.0 and CFK 3.0
2. We use 3 Kafka brokers. This is just because Control Center asks for it. We're not doing anything requiring multiple brokers in this workshop.
3. We enable the Flink UI with `confluent.controlcenter.cmf.enable` and `confluent.controlcenter.cmf.url`.
4. We're using *Schema Registry* and *Kafka Connect*. Schema Registry is required for FlinkSQL to infer metadata. *Kafka Connect* is not required but can be used to extend the workshop.

Next, install *Confluent Control Center*, *Confluent Server*, and *Connect*:
```
Add the Confluent repository and install *Confluent for Kubernetes*:
```
kubectl apply exercise-0/c3ng.yaml
```
To monitor the installation you can use:
```
watch kubectl get pods -n confluent
```
The *KRaft Controller* will install first, then *Kafka*, then *Connect*, then *Control Center*. The whole process takes a few minutes.

Port forward Control Center so you can view it at [http://localhost:9021](http://localhost:9021) (It might complain about missing Flink. This is expected):
```
kubectl -n confluent port-forward controlcenter-next-gen-0 9021:9021 > /dev/null 2>&1 &
```

###Part 2: Flink Kubernetes Operator (FKO), Confluent Manager for Apache Flink (CMF)
*Confluent Platform for Apache Flink* uses *Flink Kubernetes Application Mode* to deploy and manage its applications. Install the FLink Kubernetes Operator like so:

First install the cert manager:
```
kubectl create -f https://github.com/jetstack/cert-manager/releases/download/v1.8.2/cert-manager.yaml
```

Then install the FKO. NOTE: You'll notice we use the default namespace here, where we used the `confluent` namespace above. This isn't necessary. We just do it here so we can treat Flink separately from the rest of Confluent Platform. 
```
helm upgrade --install cp-flink-kubernetes-operator confluentinc/flink-kubernetes-operator
```

Now install Confluent Manager for Apache Flink. We'll tell it we're running in non-prod and we want to use the example SQL catalog. We won't use the catalog in this workshop but you can use it later:
```
helm upgrade --install cmf \
confluentinc/confluent-manager-for-apache-flink ---set cmf.sql.examples-catalog.enabled=true --set cmf.production=false
```

In a few minutes, Confluent Control Center should be happy with Flink. (TODO: confirm that you don't need to restart C3)

Create Kafka Topics for the rest of the exercise. We want to register schema for these topics:
1. Create a topic for the Flink input. Call it `flink-input` and use the avro schema at [exercise-1/flink-input-value.avsc](exercise-1/flink-input-value.avsc)
- Produce a sample message using [exercise-1/sample-message.json](exercise-1/sample-message.json)
2. Create a topic for the Flink output. Call it `flink-output` and use the avro schema at [exercise-1/flink-output-value.avsc](exercise-1/flink-output-value.avsc)

##Exercise 1: CMF Web UI in Confluent Control Center
1. Create an environment using the default namespace.
2. Create an application in this environment.
- Click `create an application`
- Click the link to the docs and copy the deployment spec for the sample application
3. Browse through the application instances (there's only one), events, and Apache Flink web UI for this application (TODO screenshots)
4. Edit the deployment specification (TODO: precise edit, but anything works) and re-deploy the application.
- Now see a new application instance

##Exercise 2: Confluent CLI for CP-Flink
The `confluent` CLI also supports CP-Flink. Let's use it as well:

Put up a port forward so you can use the local CLI to talk the CMF container running in Kubernetes:

```
kubectl port-forward svc/cmf-service 8080:80 > /dev/null 2>&1 &
```

Set an environment variable for the CMF URL so we don't have to add it on every CLI invocation:
```
export CONFLUENT_CMF_URL=http://localhost:8080
```

Make sure you're running Confluent CLI 4.28 or later.

```
confluent flink environment list
confluent flink application list --environment my-env
```
If you have a running application from the last exercise, you can view it:
```
confluent flink application describe basic-example --environment my-env
```

You can also submit new applications like so (TODO provide an application.json):

```
confluent flink application create application.json --environment my-env
```

##Exercise 3: Working with FlinkSQL
FlinkSQL uses *compute pools* to define resources for statements. Examine [exercise-3/compute-pool.json](exercise-3/compute-pool.json) and note what it refers to, particularly the *SQL-specific docker image*, and resources for the *JobManager* and *TaskManager*.

TODO: Use the SQL shell for these exercises when the SQL shell is available

Create a compute pool:
```
confluent flink compute-pool create ./compute-pool.json --environment my-env
```

*catalog*s are used for metadata. In the provided catalog we point the SQL service in CMF to Confluent's schema registry and the Kafka instance we're using. Examine [exercise-3/catalog.json]/(exercise-3/catalog.json)] and apply it like so:
```
confluent flink catalog create ./catalog.json
```
You can list statements like so:
```
confluent flink statement list --environment my-env --compute-pool my-pool
```
View the tables defined by catalogs like so:
```
confluent flink statement create show --environment my-env --compute-pool my-pool --catalog kcat --sql "show tables;" --output json
```
Describe a table:
```
confluent flink statement create desc --environment my-env --compute-pool my-pool --catalog kcat --sql "describe \`flink-input\`;" --output json
```
Submit a long-running query. In the absence of a SQL shell you don't see output, but we don't care:
```
confluent flink statement create selectstar --environment my-env --compute-pool my-pool --catalog kcat --sql "SELECT * FROM \`flink-input\`;" --output json
```
You can view the web UI for the statement at:
```
confluent flink statement web-ui-forward pipeline --environment my-env
```
You can also view the statement status on kubernetes. You'll see an application cluster for each query. If the query is stuck in `pending`, you may need to go back and kill any running applications.
```
kubectl get pods
kubectl logs <pod name>
```
Delete the running statement:
```
confluent flink statement delete selectstar --environment my-env
```
Create a data pipeline that reads from `flink-input`, performs some basic processing, and writes to `flink-output`. You'll notice that we're referring to `stmt-config.json`. View `stmt-config.json` for an example of how to paramaterize a SQL statement.
```
confluent flink statement create pipeline \
  --environment my-env \
  --compute-pool jwfbean-pool \
  --catalog kcat --flink-configuration ./stmt-config.json \
  --sql "INSERT INTO \`flink-output\` SELECT \`key\`, CAST(CHAR_LENGTH(val1) AS BIGINT) AS \`ecount\` FROM \`flink-input\` WHERE val1 IS NOT NULL;"
```
