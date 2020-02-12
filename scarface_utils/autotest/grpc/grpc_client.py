import grpc
import logging
import sys
import importlib

from autotest.auth.drawbridge_client import DrawBridgeClient

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)
logger = logging.getLogger("GRPC Client")


class GRPCClient(object):

    def __init__(self, access_token, grpc_host, channel, stub_client, pb_grpc_module):
        self.access_token = access_token
        self.grpc_host = grpc_host
        self.channel = channel
        self.stub_client = stub_client
        self.pb_grpc_module = pb_grpc_module

    @classmethod
    def from_config(cls, access_token, grpc_host, proto_name, stub_name):
        proto_buffer_grpc_module = importlib.import_module(
            'autotest.grpc.proto_buffers.{proto_name}_pb2_grpc'.format(proto_name=proto_name)
        )
        at_creds = grpc.access_token_call_credentials(access_token)
        ssl_creds = grpc.ssl_channel_credentials()
        channel_creds = grpc.composite_channel_credentials(ssl_creds, at_creds)
        channel = grpc.secure_channel(grpc_host, channel_creds)
        return cls(
            access_token=access_token,
            grpc_host=grpc_host,
            channel=channel,
            stub_client=getattr(proto_buffer_grpc_module, '{}Stub'.format(stub_name))(channel),
            pb_grpc_module=proto_buffer_grpc_module,
        )

    @classmethod
    def from_auth_config(cls, auth_base_url, campaign_token, credentials, grpc_host, proto_name, stub_name):
        proto_buffer_grpc_module = importlib.import_module(
            'autotest.grpc.proto_buffers.{proto_name}_pb2_grpc'.format(proto_name=proto_name)
        )
        d = DrawBridgeClient.from_env_and_campaign(auth_base_url, campaign_token,)
        d.get_jwt_token(credentials)
        d.get_access_token()

        at_creds = grpc.access_token_call_credentials(d.access_token)
        ssl_creds = grpc.ssl_channel_credentials()
        channel_creds = grpc.composite_channel_credentials(ssl_creds, at_creds)
        channel = grpc.secure_channel(grpc_host, channel_creds)
        return cls(
            access_token=d.access_token,
            grpc_host=grpc_host,
            channel=channel,
            stub_client=getattr(proto_buffer_grpc_module, '{}Stub'.format(stub_name))(channel),
            pb_grpc_module=proto_buffer_grpc_module,
        )

    def make_call(self, method_name, args, kwargs):
        grpc_call = getattr(self.stub_client, method_name)
        response = grpc_call(*args, **kwargs)
        return response
