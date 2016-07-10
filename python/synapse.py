import threading

from .event_server import EventServer
from .rpc_server import RpcServer
from .rpc_client import RpcClient


class Synapse(EventServer,RpcServer,RpcClient):
    @classmethod
    def serve(self):
        if self.app_name == "" or self.sys_name == "":
            self.log("[Synapse Error] Must Set app_name and sys_name , system exit .")
            exit(1)
        else:
            self.log("[Synapse Info] System Name: %s" % self.sys_name)
            self.log("[Synapse Info] System App Name: %s" % self.app_name)
        if self.debug:
            self.log("[Synapse Warn] System Run Mode: Debug")
        else:
            self.log("[Synapse Info] System Run Mode: Production")
        self.create_connection()
        self.create_channel()
        self.check_exchange()
        if self.event_callback_map == {}:
            self.log("[Synapse Warn] Event Server Handler Disabled: event_callback_map not set")
        else:
            threading._start_new_thread(self.event_server_serve, ())
        if self.rpc_callback_map == {}:
            self.log("[Synapse Warn] Rpc Handler Server Disabled: rpc_callback_map not set")
        else:
            threading._start_new_thread(self.rpc_server_serve, ())
        if self.disable_event_client:
            self.log("[Synapse Warn] Event Sender Disabled: disable_event_client set True")
        if self.disable_rpc_client:
            self.log("[Synapse Warn] Rpc Sender Disabled: disable_rpc_client set True")
