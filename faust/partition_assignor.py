from kafka.coordinator.assignors.abstract import AbstractPartitionAssignor


class PartitionAssignor(AbstractPartitionAssignor):
    '''PartitionAssignor to handle internal topic creation
    All topics for a given topology for a given topology being
    consumed on the same box.

    Further, this assignor needs to be sticky and potentially redundant

    Interface copied from:
    https://github.com/dpkp/kafka-python/blob/master/kafka/coordinator/assignors/abstract.py
    '''

    def metadata(self, topics):
        return super().metadata(topics)

    def assign(self, cluster, members):

        return super().assign(cluster, members)

    def on_assignment(self, assignment):
        super().on_assignment(assignment)

    @property
    def name(self):
        return "faust"
