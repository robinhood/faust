from __future__ import absolute_import

from kafka.protocol.types import (
    Array, Boolean, Int16, Int32, Schema, String,
)

from ..protocol.api import Request, Response


class CreateTopicsResponse_v0(Response):
    API_KEY = 19
    API_VERSION = 0
    SCHEMA = Schema(
        ('topic_error_codes', Array(
            ('topic', String('utf-8')),
            ('error_code', Int16)))
    )


class CreateTopicsResponse_v1(Response):
    API_KEY = 19
    API_VERSION = 1
    SCHEMA = Schema(
        ('topic_error_codes', Array(
            ('topic', String('utf-8')),
            ('error_code', Int16),
            ('error_message', String('utf-8'))))
    )


class CreateTopicsRequest_v0(Request):
    API_KEY = 19
    API_VERSION = 0
    RESPONSE_TYPE = CreateTopicsResponse_v0
    SCHEMA = Schema(
        ('create_topic_requests', Array(
            ('topic', String('utf-8')),
            ('num_partitions', Int32),
            ('replication_factor', Int16),
            ('replica_assignment', Array(
                ('partition_id', Int32),
                ('replicas', Array(Int32)))),
            ('configs', Array(
                ('config_key', String('utf-8')),
                ('config_value', String('utf-8')))))),
        ('timeout', Int32)
    )


class CreateTopicsRequest_v1(Request):
    API_KEY = 19
    API_VERSION = 1
    RESPONSE_TYPE = CreateTopicsResponse_v1
    SCHEMA = Schema(
        ('create_topic_requests', Array(
            ('topic', String('utf-8')),
            ('num_partitions', Int32),
            ('replication_factor', Int16),
            ('replica_assignment', Array(
                ('partition_id', Int32),
                ('replicas', Array(Int32)))),
            ('configs', Array(
                ('config_key', String('utf-8')),
                ('config_value', String('utf-8')))))),
        ('timeout', Int32),
        ('validate_only', Boolean)
    )


CreateTopicsRequest = [CreateTopicsRequest_v0, CreateTopicsRequest_v1]
CreateTopicsResponse = [CreateTopicsResponse_v0, CreateTopicsRequest_v1]
