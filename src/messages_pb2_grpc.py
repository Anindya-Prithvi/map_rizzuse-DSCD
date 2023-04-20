# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
"""Client and server classes corresponding to protobuf-defined services."""
import grpc

import messages_pb2 as messages__pb2


class MapProcessInputStub(object):
    """/ Some service"""

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.Receive = channel.unary_unary(
            "/MapProcessInput/Receive",
            request_serializer=messages__pb2.InputMessage.SerializeToString,
            response_deserializer=messages__pb2.Success.FromString,
        )


class MapProcessInputServicer(object):
    """/ Some service"""

    def Receive(self, request, context):
        """/ Some method"""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("Method not implemented!")
        raise NotImplementedError("Method not implemented!")


def add_MapProcessInputServicer_to_server(servicer, server):
    rpc_method_handlers = {
        "Receive": grpc.unary_unary_rpc_method_handler(
            servicer.Receive,
            request_deserializer=messages__pb2.InputMessage.FromString,
            response_serializer=messages__pb2.Success.SerializeToString,
        ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
        "MapProcessInput", rpc_method_handlers
    )
    server.add_generic_rpc_handlers((generic_handler,))


# This class is part of an EXPERIMENTAL API.
class MapProcessInput(object):
    """/ Some service"""

    @staticmethod
    def Receive(
        request,
        target,
        options=(),
        channel_credentials=None,
        call_credentials=None,
        insecure=False,
        compression=None,
        wait_for_ready=None,
        timeout=None,
        metadata=None,
    ):
        return grpc.experimental.unary_unary(
            request,
            target,
            "/MapProcessInput/Receive",
            messages__pb2.InputMessage.SerializeToString,
            messages__pb2.Success.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
        )


class ReduceProcessInputStub(object):
    """Missing associated documentation comment in .proto file."""

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.Receive = channel.unary_unary(
            "/ReduceProcessInput/Receive",
            request_serializer=messages__pb2.InputMessage.SerializeToString,
            response_deserializer=messages__pb2.Success.FromString,
        )


class ReduceProcessInputServicer(object):
    """Missing associated documentation comment in .proto file."""

    def Receive(self, request, context):
        """/ Some method"""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("Method not implemented!")
        raise NotImplementedError("Method not implemented!")


def add_ReduceProcessInputServicer_to_server(servicer, server):
    rpc_method_handlers = {
        "Receive": grpc.unary_unary_rpc_method_handler(
            servicer.Receive,
            request_deserializer=messages__pb2.InputMessage.FromString,
            response_serializer=messages__pb2.Success.SerializeToString,
        ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
        "ReduceProcessInput", rpc_method_handlers
    )
    server.add_generic_rpc_handlers((generic_handler,))


# This class is part of an EXPERIMENTAL API.
class ReduceProcessInput(object):
    """Missing associated documentation comment in .proto file."""

    @staticmethod
    def Receive(
        request,
        target,
        options=(),
        channel_credentials=None,
        call_credentials=None,
        insecure=False,
        compression=None,
        wait_for_ready=None,
        timeout=None,
        metadata=None,
    ):
        return grpc.experimental.unary_unary(
            request,
            target,
            "/ReduceProcessInput/Receive",
            messages__pb2.InputMessage.SerializeToString,
            messages__pb2.Success.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
        )
