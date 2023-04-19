# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
"""Client and server classes corresponding to protobuf-defined services."""
import grpc

import database_pb2 as database__pb2


class databaseStub(object):
    """Missing associated documentation comment in .proto file."""

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.queryDatabase = channel.unary_unary(
                '/database/queryDatabase',
                request_serializer=database__pb2.databaseRequest.SerializeToString,
                response_deserializer=database__pb2.databaseResponse.FromString,
                )
        self.executeClientRequest = channel.unary_unary(
                '/database/executeClientRequest',
                request_serializer=database__pb2.databaseRequest.SerializeToString,
                response_deserializer=database__pb2.databaseResponse.FromString,
                )


class databaseServicer(object):
    """Missing associated documentation comment in .proto file."""

    def queryDatabase(self, request, context):
        """Query the database
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def executeClientRequest(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')


def add_databaseServicer_to_server(servicer, server):
    rpc_method_handlers = {
            'queryDatabase': grpc.unary_unary_rpc_method_handler(
                    servicer.queryDatabase,
                    request_deserializer=database__pb2.databaseRequest.FromString,
                    response_serializer=database__pb2.databaseResponse.SerializeToString,
            ),
            'executeClientRequest': grpc.unary_unary_rpc_method_handler(
                    servicer.executeClientRequest,
                    request_deserializer=database__pb2.databaseRequest.FromString,
                    response_serializer=database__pb2.databaseResponse.SerializeToString,
            ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
            'database', rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))


 # This class is part of an EXPERIMENTAL API.
class database(object):
    """Missing associated documentation comment in .proto file."""

    @staticmethod
    def queryDatabase(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/database/queryDatabase',
            database__pb2.databaseRequest.SerializeToString,
            database__pb2.databaseResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def executeClientRequest(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/database/executeClientRequest',
            database__pb2.databaseRequest.SerializeToString,
            database__pb2.databaseResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)
