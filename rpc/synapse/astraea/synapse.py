from __future__ import print_function
import time
import pika
import threading
from random import Random
from rpc.synapse.astraea.event_server import EventServer
from rpc.synapse.astraea.event_client import EventClient
from rpc.synapse.astraea.rpc_server import RpcServer
from rpc.synapse.astraea.rpc_client import RpcClient


class Synapse:
    debug = False
    disable_rpc_client = False
    disable_event_client = False
    sys_name = ''
    app_name = ''
    app_id = ''
    mq_host = ''
    mq_port = 5672
    mq_user = ''
    mq_pass = ''
    mq_vhost = '/'
    event_process_num = 20
    rpc_process_num = 20
    rpc_timeout = 3
    event_callback = {}
    rpc_callback = {}

    LogInfo = "Info"
    LogWarn = "Warn"
    LogDebug = "Debug"
    LogError = "Error"

    _rpc_client = None
    _event_client = None

    def serve(self):
        if self.app_name == "" or self.sys_name == "":
            self.log("Must Set app_name and sys_name , system exit .", self.LogError)
            exit(1)
        else:
            self.log("System Name: %s" % self.sys_name)
            self.log("App Name: %s" % self.app_name)
        if self.app_id == "":
            self.app_id = self.random_string()
            self.log("App Id: %s" % self.app_id)
        if self.debug:
            self.log("App Run Mode: Debug", self.LogWarn)
        else:
            self.log("App Run Mode: Production")
        self._check_and_create_exchange()
        if self.event_callback == {}:
            self.log("Event Server Disabled: event_callback not set", self.LogWarn)
        else:
            threading.Thread(target=EventServer(self).run).start()
            for k in self.event_callback:
                self.log("*EVT: %s -> %s" % (k, self.event_callback[k].__name__))
        if self.rpc_callback == {}:
            self.log("Rpc Server Disabled: rpc_callback not set", self.LogWarn)
        else:
            threading.Thread(target=RpcServer(self).run).start()
            for k in self.rpc_callback:
                self.log("*RPC: %s -> %s" % (k, self.rpc_callback[k].__name__))
        if self.disable_event_client:
            self.log("Event Client Disabled: disable_event_client set True", self.LogWarn)
        else:
            self._event_client = EventClient(self)
        if self.disable_rpc_client:
            self.log("Rpc Client Disabled: disable_rpc_client set True", self.LogWarn)
        else:
            self._rpc_client = RpcClient(self)
            threading.Thread(target=self._rpc_client.run).start()

    def send_event(self, event, params):
        if self.disable_event_client:
            self.log("Event Client Disabled!", self.LogError)
        self._event_client.send(event, params)

    def send_rpc(self, app, method, params):
        if self.disable_rpc_client:
            self.log("Rpc Client Disabled!", self.LogError)
            res = {"rpc_error": "rpc client disabled"}
        else:
            res = self._rpc_client.send(app, method, params)
        return res

    def _create_connection(self, desc='unknow'):
        config = pika.ConnectionParameters(
            host=self.mq_host,
            port=self.mq_port,
            virtual_host=self.mq_vhost,
            credentials=pika.PlainCredentials(self.mq_user, self.mq_pass),
            client_properties={"connection_name": "%s_%s_%s_%s" % (self.sys_name, self.app_name, self.app_id, desc)}
        )
        conn = pika.BlockingConnection(parameters=config)
        self.log("%s Connection Created" % desc)
        return conn

    def create_channel(self, process_num=0, desc="unknow", connection=False):
        if not connection:
            connection = self._create_connection(desc=desc)
        channel = connection.channel()
        self.log("%s Channel Created" % desc)
        if process_num > 0:
            channel.basic_qos(prefetch_size=0, prefetch_count=process_num)
            self.log("%s MaxProcessNum: %d" % (desc, process_num))
        return channel

    def _check_and_create_exchange(self):
        connection = self._create_connection(desc="Exchange")
        channel = self.create_channel(desc="Exchange", connection=connection)
        channel.exchange_declare(self.sys_name, 'topic', durable=True, auto_delete=True)
        self.log("Register Exchange Successed.")
        connection.close()
        self.log("Exchange Channel Closed")
        self.log("Exchange Connection Closed")

    def random_string(self, lens=20):
        str = ''
        chars = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ'
        length = len(chars) - 1
        random = Random()
        for i in range(lens):
            str += chars[random.randint(0, length)]
        return str

    def log(self, desc, level=LogInfo):
        print("[%s][Synapse %s] %s" % (time.strftime("%Y-%m-%d %H:%M:%S"), level, desc))
