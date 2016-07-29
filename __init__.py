#!/usr/bin/env python
# ~*~ coding: utf-8 ~*~
"""
    synapse
    ~~~~~~~

    It's a sdk tool for python send/receive rpc/event with other module/app.

    :copyright: (c) 2014-2016 Jumpserver Team
    :Author: ibuler@qq.com
    :License: GNU v2, see LICENSE for more details.
"""

import pika
import uuid
import json
import logging


class RpcClient(object):
    """Rcp client class for app request other module.

    :param mq_host: Nothing to say, it's so obvious
    :param mq_port: Look upside
    :param mq_user: Look upside
    :param mq_pass: Look upside
    :param client_name: It's have little affect except for logging
    :param sys_name: It's will be use a exchange name of MQ  default JMS
    """

    def __init__(self, mq_host='localhost', mq_port=5672, mq_user='', mq_pass='', client_name=None, sys_name=None):
        self.mq_host = mq_host
        self.mq_port = mq_port
        self.mq_user = mq_user
        self.mq_pass = mq_pass
        # make a connection with MQ server.
        self.connection = self.make_connection()
        self.channel = self.get_channel()
        self.response = {}

        if client_name is None:
            self.client_name = str(uuid.uuid4())
        else:
            self.client_name = client_name

        if sys_name is None:
            self.sys_name = 'JMS'
        else:
            self.sys_name = sys_name

    def make_connection(self):
        self.connection = rv = pika.BlockingConnection(pika.ConnectionParameters(
                host=self.mq_host, port=self.mq_port,
                credentials=pika.PlainCredentials(username=self.mq_user,
                                                  password=self.mq_pass)
                ))
        return rv

    def get_channel(self):
        self.channel = rv = self.connection.channel()
        return rv

    def call(self, app=None, action=None, params=None):
        """Make a request and send to mq.

        Use params to create a request, request is a dict and can be serialized as json,
        example {'app': 'cmdb', 'action': 'asset_add', 'params': {'id': 123, 'name': 'localhost', ...}

        :param app: which app will be process this request, RPC server bind this
        :param action: It's a function will be call for remote app
        :param params: It's the params will be used by function

        """
        if app is None:
            logging.error('Param `app` should be a passed')
            return {'error': 'Param `app` should be a passed'}

        if action is None:
            logging.error('Param `action` should be a passed')
            return {'error': 'Param `action` should be a passed'}

        if params is None:
            params = {}

        if not isinstance(params, dict):
            logging.error('Param `params` should be a dict')
            return {'error': 'Param `request` should be a dict'}

        request = {
            'from': self.client_name,
            'to': app,
            'action': action,
            'params': params,
        }
        try:
            request = json.dumps(request)
        except TypeError:
            logging.error('`request` should be serialize failed')
            return {'error': '`request` should be serialize failed'}

        logging.info('Send request to: %s call action: %s params: %s' % (app, action, params))
        corr_id = str(uuid.uuid4())
        self.response[corr_id] = None

        def on_response(ch, method, props, body):
            if corr_id == props.correlation_id:
                self.response[corr_id] = body

        #: define a unique queue for receive response
        callback_queue = self.channel.queue_declare(exclusive=True).method.queue

        #: Bind rpc server response msg to our client unique queue
        self.channel.queue_bind(exchange=self.sys_name,
                                queue=callback_queue,
                                routing_key=callback_queue)

        #: Consume server response msg
        self.channel.basic_consume(on_response, no_ack=True,
                                   queue=callback_queue)

        #: Publish request to MQ
        self.channel.basic_publish(exchange=self.sys_name,
                                   routing_key=app,
                                   properties=pika.BasicProperties(
                                       reply_to=callback_queue,
                                       correlation_id=corr_id
                                   ),
                                   body=request)

        while self.response.get(corr_id, None) is None:
            self.connection.process_data_events(time_limit=2)
        rv = self.response.pop(corr_id)
        logging.info('Request <%s> get response %s.' % (corr_id, rv,))
        return rv


if __name__ == '__main__':
    import threading
    logging.basicConfig(level=logging.INFO)
    rpc_client = RpcClient()
    threads = []
    for i in range(1, 5):
        t = threading.Thread(target=rpc_client.call, args=('cmdb', 'action', {'hello': 'world'}))
        t.start()
        threads.append(t)

    for i in threads:
        i.join()
