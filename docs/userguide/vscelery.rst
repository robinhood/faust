.. _guide-vscelery:

================
 Faust x Celery
================

Faust is a stream processor, so what does it have in common with Celery?

If you've used tools such as Celery in the past, you can think of Faust as being able
to, not only run tasks, but for tasks to keep history of everything that has
happened so far. That is tasks ("agents" in Faust) can keep state, and also
replicate that state to a cluster of Faust worker instances.

It uses Kafka as a broker, but Kafka behaves differently from the queues
you may know from brokers using AMQP/Redis/Amazon SQS/and so on. Or rather, it's
not a queue at all, instead it's a log structure, so you can go forwards
and backwards in time to retrieve the history of messages sent. Kafka has
"replayability", in that if something goes wrong, you could actually stop the
world and rewind.
