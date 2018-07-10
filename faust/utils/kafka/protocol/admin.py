"""Admin related Kafka protocol extensions."""
from rhkafka.protocol import types
from .api import Request, Response


class CreateTopicsResponse_v0(Response):
    """Response from Create Topic request (version 0)."""

    API_KEY = 19
    API_VERSION = 0
    SCHEMA = types.Schema(
        ('topic_error_codes', types.Array(
            ('topic', types.String('utf-8')),
            ('error_code', types.Int16))),
    )


class CreateTopicsResponse_v1(Response):
    """Response from Create Topic request (version 1)."""

    API_KEY = 19
    API_VERSION = 1
    SCHEMA = types.Schema(
        ('topic_error_codes', types.Array(
            ('topic', types.String('utf-8')),
            ('error_code', types.Int16),
            ('error_message', types.String('utf-8')))),
    )


class CreateTopicsRequest_v0(Request):
    """Request to create topic (version 0)."""

    API_KEY = 19
    API_VERSION = 0
    RESPONSE_TYPE = CreateTopicsResponse_v0
    SCHEMA = types.Schema(
        ('create_topic_requests', types.Array(
            ('topic', types.String('utf-8')),
            ('num_partitions', types.Int32),
            ('replication_factor', types.Int16),
            ('replica_assignment', types.Array(
                ('partition_id', types.Int32),
                ('replicas', types.Array(types.Int32)))),
            ('configs', types.Array(
                ('config_key', types.String('utf-8')),
                ('config_value', types.String('utf-8')))))),
        ('timeout', types.Int32),
    )


class CreateTopicsRequest_v1(Request):
    """Request to create topic (version 1)."""

    API_KEY = 19
    API_VERSION = 1
    RESPONSE_TYPE = CreateTopicsResponse_v1
    SCHEMA = types.Schema(
        ('create_topic_requests', types.Array(
            ('topic', types.String('utf-8')),
            ('num_partitions', types.Int32),
            ('replication_factor', types.Int16),
            ('replica_assignment', types.Array(
                ('partition_id', types.Int32),
                ('replicas', types.Array(types.Int32)))),
            ('configs', types.Array(
                ('config_key', types.String('utf-8')),
                ('config_value', types.String('utf-8')))))),
        ('timeout', types.Int32),
        ('validate_only', types.Boolean),
    )


CreateTopicsRequest = [CreateTopicsRequest_v0, CreateTopicsRequest_v1]
CreateTopicsResponse = [CreateTopicsResponse_v0, CreateTopicsRequest_v1]
