import time
from kombu import Connection, Exchange
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
    proccess_num = 100
    event_callback_map = {}
    rpc_callback_map = {}
    rpc_cli_results = {}

    mqex = None
    is_server = False

    def log(self, msg):
        print(time.strftime("%Y/%m/%d %H:%M:%S"), msg)

    def create_connection(self):
        if self.mq_user is None:
            amqp_uri = 'amqp://%s:%s//' % (self.mq_host, self.mq_port)
        else:
            amqp_uri = 'amqp://%s:%s@%s:%s//' % (self.mq_user, self.mq_pass, self.mq_host, self.mq_port)
        self.conn = Connection(amqp_uri, insist=True, ssl=False)

    def check_exchange(self):
        self.mqex = Exchange(self.sys_name, 'topic', durable=True)

    def random_str(self, str_len=20):
        str = ''
        chars = 'AaBbCcDdEeFfGgHhIiJjKkLlMmNnOoPpQqRrSsTtUuVvWwXxYyZz0123456789'
        length = len(chars) - 1
        random = Random()
        for i in range(str_len):
            str += chars[random.randint(0, length)]
        return str
