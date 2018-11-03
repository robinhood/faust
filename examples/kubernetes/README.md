## Faust example using kubernetes

Example showcasing how to use Faust workers inside a Kubernetes cluster.
This example will only work on a Mac running a docker version that supports
_[Kubernetes in Docker for a Mac](https://docs.docker.com/docker-for-mac/kubernetes/)_.

Once [Docker for Mac](https://store.docker.com/editions/community/docker-ce-desktop-mac)
has been downloaded, enable a local Kubernetes cluster to start developing.

### Architecture

1. This example assumes a local Kafka cluster running locally with all the
default settings

2. The producer sends 10,000 messages to the Kafka cluster. In the spirit of
distributed systems, the producer doesn't use faust, it uses another python library
to producer messages. The rational for not using faust is to showcase how faust can
be added as a new component to the overall system.

3. Consumer. This is a Faust worker, consuming the messages placed in the Kafka topic,
which we have named `test`


### Steps to run the example.

1. Start the Kafka cluster locally.
2. Start the Kubernetes cluster locally using Docker for Mac.
3. Build the producer docker image `docker build producer/ -t producer`
4. Run the producer as a [Kubernetes Job](https://kubernetes.io/docs/concepts/workloads/controllers/jobs-run-to-completion/) `kubectl apply -f producer/producer.yaml`
5. Build the consumer docker image `docker build consumer/ -t consumer`
6. Run the consumer as a [Kubernetes Deployment](https://kubernetes.io/docs/concepts/workloads/controllers/deployment/) `kubectl apply -f consumer/consumer.yaml`
7. To re-run the producer:
```bash
4. To send a second batch of 10,000 messages
```bash
kubectl delete -f producer.yml
kubectl apply -f producer.yml
```

_For additional information on the producer and consumer please refer to its README.md_
