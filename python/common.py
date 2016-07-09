import threading

from .event_server import EventServer
from .rpc_server import RpcServer


class Synapse(EventServer,RpcServer):
    @classmethod
    def serve(self):
        if self.app_name == "" or self.sys_name == "":
            self.log("[Synapse Error] Must Set app_name and sys_name , system exit .")
            exit(1)
        else:
            self.log("[Synapse Info] System App Name: %s" % self.app_name)
        if self.debug:
            self.log("[Synapse Warn] System Run Mode: Debug")
        else:
            self.log("[Synapse Info] System Run Mode: Production")
        self.create_channel()
        self.check_exchange()
        if self.event_callback_map == {}:
            self.log("[Synapse Warn] Event Handler Disabled: event_callback_map not set")
        else:
            threading._start_new_thread(self.event_serve, ())
        if self.rpc_callback_map == {}:
            self.log("[Synapse Warn] Rpc Handler Disabled: rpc_callback_map not set")
        else:
            threading._start_new_thread(self.rpc_serve, ())
        if self.disable_event_client:
            self.log("[Synapse Warn] Event Sender Disabled: disable_event_client set True")
        if self.disable_rpc_client:
            self.log("[Synapse Warn] Rpc Sender Disabled: disable_rpc_client set True")

    @classmethod
    def send_event(self, action, params):
        print("send a event", action, params)

    @classmethod
    def send_rpc(self, action, params):
        print("send a event", action, params)
