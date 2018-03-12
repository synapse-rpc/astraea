from __future__ import print_function
import time
from kombu import Connection, Exchange
from random import Random


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
    rpc_cli_results = {}

    LogInfo = "Info"
    LogWarn = "Warn"
    LogDebug = "Debug"
    LogError = "Error"

    conn = None
    mqex = None
    is_server = False

    def log(self, msg, level=LogInfo):
        print("[%s][Synapse %s] %s" % (time.strftime("%Y-%m-%d %H:%M:%S"), level, msg))

    def create_connection(self):
        self.conn = Connection(
            "amqp://%s:%s@%s:%s/%s" % (self.mq_user, self.mq_pass, self.mq_host, self.mq_port, self.mq_vhost),
            insist=True, ssl=False)
        self.log("Rabbit MQ Connection Created.")

    def create_channel(self, process_num=0, desc="unknow"):
        channel = self.conn.channel()
        self.log("%s Channel Created" % desc)
        if process_num > 0:
            channel.basic_qos(prefetch_size=0, prefetch_count=process_num, a_global=False)
            self.log("%s MaxProcessNum: %d" % (desc, process_num))
        return channel

    def check_and_create_exchange(self):
        channel = self.create_channel(desc="Exchange")
        self.mqex = Exchange(self.sys_name, 'topic', channel=channel, durable=True, auto_delete=True)
        self.mqex.declare()
        self.log("Register Exchange Successed.")
        channel.close()
        self.log("Exchange Channel Closed")

    def random_str(self, str_len=20):
        str = ''
        chars = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ'
        length = len(chars) - 1
        random = Random()
        for i in range(str_len):
            str += chars[random.randint(0, length)]
        return str

    def reconnect(self):
        self.conn.release()
        self.serve()
