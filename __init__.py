from .event_server import EventServer
from .event_client import EventClient
from .rpc_server import RpcServer
from .rpc_client import RpcClient


class Synapse(EventServer, EventClient, RpcServer, RpcClient):
    def __init__(self, app_name='', app_id='', sys_name='', mq_host='', mq_port='', mq_user='',
                 mq_pass='', debug=False,
                 disable_rpc_client=False, disable_event_client=False, event_callback_map={},
                 rpc_callback_map={}):
        self.app_name = app_name
        self.sys_name = sys_name
        self.mq_host = mq_host
        self.mq_port = mq_port
        self.mq_user = mq_user
        self.mq_pass = mq_pass
        self.app_id = app_id
        self.debug = debug
        self.disable_rpc_client = disable_rpc_client
        self.disable_event_client = disable_event_client
        self.event_callback_map = event_callback_map
        self.event_callback_map = rpc_callback_map

    def __del__(self):
        self.conn.release()

    def serve(self):
        self.serve_handler()
        while self.is_server:
            try:
                self.conn.drain_events()
            except:
                self.log("[Synapse Error] System Connection Lost, Reconnect... ")
                self.conn.release()
                self.serve_handler()

    def serve_handler(self):
        if self.app_name == "" or self.sys_name == "":
            self.log("[Synapse Error] Must Set app_name and sys_name , system exit .")
            exit(1)
        else:
            self.log("[Synapse Info] System Name: %s" % self.sys_name)
            self.log("[Synapse Info] System App Name: %s" % self.app_name)
            self.log("[Synapse Info] App MaxProcessNum: %d" % self.proccess_num)
        if self.debug:
            self.log("[Synapse Warn] System Run Mode: Debug")
        else:
            self.log("[Synapse Info] System Run Mode: Production")
        if self.app_id == "":
            self.app_id = self.random_str()
            self.log("[Synapse Info] System App Id: %s" % self.app_id)
        self.create_connection()
        self.check_exchange()
        if self.event_callback_map == {}:
            self.log("[Synapse Warn] Event Server Handler Disabled: event_callback_map not set")
        else:
            self.is_server = True
            self.event_server_serve()
            for k in self.event_callback_map:
                self.log("[Synapse Info] *ENT: %s -> %s" % (k, self.event_callback_map[k].__name__))
        if self.rpc_callback_map == {}:
            self.log("[Synapse Warn] Rpc Handler Server Disabled: rpc_callback_map not set")
        else:
            self.is_server = True
            self.rpc_server_serve()
            for k in self.rpc_callback_map:
                self.log("[Synapse Info] *RPC: %s -> %s" % (k, self.rpc_callback_map[k].__name__))
        if self.disable_event_client:
            self.log("[Synapse Warn] Event Sender Disabled: disable_event_client set True")
        else:
            self.log("[Synapse Info] Event Sender Ready")
        if self.disable_rpc_client:
            self.log("[Synapse Warn] Rpc Sender Disabled: disable_rpc_client set True")
        else:
            self.is_server = True
            self.rpc_client_serve()
        if self.is_server:
            self.conn.ensure_connection(self.reconnect)
