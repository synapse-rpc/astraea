from __future__ import print_function
import time
import pika
import threading
from random import Random
from event_server import EventServer
from event_client import EventClient
from rpc_server import RpcServer
from rpc_client import RpcClient


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

    conn = None

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
        self.create_connection()
        self.check_and_create_exchange()
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
            if self.disable_rpc_client:
                self.log("Rpc Client Disabled: disable_rpc_client set True", self.LogWarn)
            else:
                pass

    def send_event(self, event, params):
        pass

    def send_rpc(self, app, method, params):
        pass

    def create_connection(self):
        config = pika.ConnectionParameters(
            host=self.mq_host,
            port=self.mq_port,
            virtual_host=self.mq_vhost,
            credentials=pika.PlainCredentials(self.mq_user, self.mq_pass)
        )
        self.conn = pika.SelectConnection(parameters=config)

    def create_channel(self, process_num=0, desc="unknow"):
        channel = self.conn.channel()
        self.log("%s Channel Created" % desc)
        if process_num > 0:
            channel.basic_qos(prefetch_size=0, prefetch_count=process_num)
            self.log("%s MaxProcessNum: %d" % (desc, process_num))
        return channel

    def check_and_create_exchange(self):
        channel = self.create_channel(desc="Exchange")
        channel.exchange_declare(self.sys_name, 'topic', durable=True, auto_delete=True)
        self.log("Register Exchange Successed.")
        channel.close()
        self.log("Exchange Channel Closed")
        pass

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
