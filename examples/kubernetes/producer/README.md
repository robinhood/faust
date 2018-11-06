## Produce 10,000 messages

This process assumes a Kafka broker accessible at `host.docker.internal:9092`


Producer that places 10,000 messages into a Kafka topic named "test". Each message
is a mapping representing a score. Each score has 2 keys:
* index
* value


1. Build the producer docker image
```bash
cd producer
# -t producer => names the image producer, is used in kubernetes yaml file
docker build . -t producer
```

2. Apply the producer as a Kubernetes job.
```bash
kubectl apply -f producer.yml
```

3. Log producer output
```bash
# Discover the pod running the job, e.g. producer-32452
kubectl get pods

kubectl logs -f <pod_running_job>
```

4. To send a second batch of 10,000 messages
```bash
kubectl delete -f producer.yml
kubectl apply -f producer.yml
```
