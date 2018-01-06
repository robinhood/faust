.. _partition-assignor:

========================
 Partition Assignor
========================

Kafka Streams
==============

Kafka Streams distributes work across multiple processes by using the
consumer group protocol introduced in Kafka 0.9.0. Kafka elects one of the
consumers in the consumer group to use its partition assignment strategy to
assign partitions to the consumers in the group. The leader gets access to
every client's subscriptions and assigns partitions accordingly.

Kafka Streams uses a sticky partition assignment strategy to minimize movement
in the case of rebalancing. Further, it is also redundant in its partition
assignment in the sense that it assigns some standby tasks to maintain state
store replicas.

The `StreamPartitionAssignor
<https://github.com/apache/kafka/blob/trunk/streams/src/main/java/org/apache/kafka/streams/processor/internals/StreamPartitionAssignor.java>`_
used by Kafka Streams works as follows:

1. Check all repartition source topics and use internal topic manager to make
   sure they have been created with the right number of partitions.

2. Using customized partition grouper (`DefaultPartitionGrouper <https://github.com/apache/kafka/blob/4b3ea062be515bc173f6c788c4c1e14f77935aef/streams/src/main/java/org/apache/kafka/streams/processor/DefaultPartitionGrouper.java>`_)
   to generate tasks along with their assigned partitions; also make sure that
   the task's corresponding changelog topics have been created with the right
   number of partitions.

3. Using `StickyTaskAssignor <https://github.com/apache/kafka/blob/4b3ea062be515bc173f6c788c4c1e14f77935aef/streams/src/main/java/org/apache/kafka/streams/processor/internals/assignment/StickyTaskAssignor.java>`_
   to assign tasks to consumer clients.

   - Assign a task to a client which was running it previously.
     If there is no such client, assign a task to a client which has its valid
     local state.
   - A client may have more than one stream threads. The assignor tries to
     assign tasks to a client proportionally to the number of threads.
   - Try not to assign the same set of tasks to two different clients

   The assignment is done in one-pass. The result may not satisfy above all.

4. Within each client, tasks are assigned to consumer clients in round-robin
   manner.

Faust
=====

Faust differs from Kafka Streams in some fundamental ways one of which is
that a task in Faust differs from a task in Kafka Streams. Further, Faust
doesn't have the concept of a pre-defined topology and subscribes to streams as
and when required in the application.

As a result, the ``PartitionAssignor`` in Faust can get rid of steps one and
two mentioned above and rely on the primitives repartitioning streams and
creating changelog topics to create topics with the correct number of
partitions based on the source topics.

We can largely simplify step three above since there is no concept of task as in
Kafka Streams, i.e. we do not introspect the application topology to define a
task that would be assigned to the clients. We simply need to make sure that
the correct partitions are assigned to the clients and the client streams and
processors should handle dealing with the co-partitioning while processing
the streams and forwarding data between the different processors.

``PartitionGrouper``
--------------------

This can be simplified immensely by grouping the same partition numbers onto
the same clients for all topics with the same number of partitions. This way
we can guarantee that co-partitioning for all topics requiring
co-partitioning (ex: in the case of joins and aggregates) as long as the
topics have the correct number of partitions (which we are making the
processors implicitly guarantee).

``StickyAssignor``
------------------

With our simple `PartitionGrouper` we can use a ``StickyPartitionAssignor`` to
assign partitions to the clients. However we need to explicitly handle
standby assignments here. We use the ``StickyPartitionAssignor`` design
approved in `KIP-54`_ as the basis for our sticky assignor.

.. _`KIP-54`:
    https://cwiki.apache.org/confluence/display/KAFKA/KIP-54+-+Sticky+Partition+Assignment+Strategy

Concerns
========

With the above design we need to be careful around the following concerns:

- We need to assign a partition (where changelog) is involved to a client
  which contains a standby replica for the given topic/partition whenever
  possible. This can result in unbalanced assignment. We can fix this by evenly
  and randomly distributing standbys such that over the long term each
  rebalance will cause the partitions being re-assigned be evenly balanced
  across all clients.

- Network Partitions and other distributed systems failure cases - We delegate
  this to the Kafka protocol. The Kafka Consumer Protocol handles a lot of the
  failure conditions involved with the Consumer group leader election such as
  leader failures, node failures, etc. Network Partitions in Kafka are not
  handled here as those would result in bigger issues than consumer partition
  assignment issues.
