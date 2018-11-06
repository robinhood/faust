## Consume 10,000 messages

This process assumes a Kafka broker accessible at `host.docker.internal:9092`


Consumes 10,000 messages from Kafka topic named "test". Each message
is a mapping representing a score. Each score has 2 keys:
* index
* value


1. Build the consumer docker image
```bash
cd consumer
# -t consumer => names the image consumer, is used in kubernetes yaml file
docker build . -t consumer
```

2. Apply the consumer as a Kubernetes deployment.
```bash
kubectl apply -f consumer.yml
```

3. Log consumer output
```bash
# Discover the pod running the job, e.g. consumer-32452
kubectl get pods

kubectl logs -f <pod_running_job>
```
