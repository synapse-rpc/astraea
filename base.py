# coding: utf-8

import time
import pika
from random import Random


class Base:
    debug = False
    disable_rpc_client = False
    disable_event_client = False
    sys_name = ''
    app_name = ''
    app_id = ''
    mq_host = ''
    mq_port = 0
    mq_user = ''
    mq_pass = ''
    event_callback_map = {}
    rpc_callback_map = {}
    rpc_cli_results = {}

    mqch = None

    @classmethod
    def log(self, msg):
        print(time.strftime("%Y/%m/%d %H:%M:%S"), msg)

    def __init__(self, app_name, app_id, sys_name, mq_host, mq_port, mq_user, mq_pass, debug,
                 disable_rpc_client, disable_event_client, event_callback_map, rpc_callback_map):
        self.app_name = app_name
        self.sys_name = sys_name
        self.mq_host = mq_host
        self.mq_port = mq_port
        self.mq_user = mq_user
        self.mq_pass = mq_pass
        self.app_id = app_id
        self.debug = True if debug else False
        self.disable_rpc_client = False if disable_rpc_client else True
        self.disable_event_client = False if disable_event_client else True
        self.event_callback_map = event_callback_map if event_callback_map else False
        self.event_callback_map = rpc_callback_map if rpc_callback_map else False

    @classmethod
    def create_connection(self):
        self.conn = pika.BlockingConnection(pika.ConnectionParameters(
            host=self.mq_host,
            port=self.mq_port,
            credentials=pika.PlainCredentials(username=self.mq_user, password=self.mq_pass)
        ))

    @classmethod
    def create_channel(self):
        self.mqch = self.conn.channel()

    @classmethod
    def check_exchange(self):
        self.mqch.exchange_declare(exchange=self.sys_name,
                                   type='topic',
                                   durable=True
                                   )

    @classmethod
    def random_str(self, str_len=20):
        str = ''
        chars = 'AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZz0123456789'
        length = len(chars) - 1
        random = Random()
        for i in range(str_len):
            str += chars[random.randint(0, length)]
        return str
