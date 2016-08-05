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

from __future__ import absolute_import, unicode_literals

import json
import logging
import threading
import socket
from multiprocessing import Process

from kombu import Exchange, Queue, Connection, Consumer
from kombu.mixins import ConsumerMixin
from kombu.pools import producers
from kombu.utils import uuid


class Synapse(object):
    default_config = {
        'MQ_HOST': 'localhost',
        'MQ_PORT': 5672,
        'MQ_USER': None,
        'MQ_PASSWORD': None,
        'DEBUG': False
    }

    def __init__(self, app_name, app_id=None):
        self.app_name = app_name
        self.app_id = app_id or uuid()
        self.config = self.default_config
        self.connection = None
        self.rpc_server = None
        self.rpc_client = None
        self.event_server = None
        self.event_client = None

    def send_rpc(self, request_app=None, action=None, params=None, timeout=3):
        self.rpc_client.send_rpc(request_app, action=action, params=params, timeout=timeout)

    def send_event(self):
        pass

    def make_connection(self):
        if self.config.get('MQ_USER', None) is None:
            amqp_uri = 'amqp://%s:%s//' % (self.config.get('MQ_HOST', 'localhost'),
                                           self.config.get('MQ_PORT', 5672))
        else:
            amqp_uri = 'amqp://%s:%s@%s:%s//' % (self.config.get('MQ_USER'),
                                                 self.config.get('MQ_PASSWORD'),
                                                 self.config.get('MQ_HOST', 'localhost'),
                                                 self.config.get('MQ_PORT', 5672))

        self.connection = rv = Connection(amqp_uri, insist=True, ssl=False)
        assert rv is not None, "Create connection will MQ server error"
        return rv

    def get_channel(self):
        if isinstance(self.connection, Connection) and self.connection.connected:
            return self.connection.channel()

    def run(self, process_num=3):
        self.make_connection()
        processes = []
        try:
            for j in range(process_num):
                process = Process(target=self.rpc_server.run, args=())
                process.start()
                processes.append(process)
                print('[%s] Awaiting RPC requests' % (j,))

            for p in processes:
                p.join()

        finally:
            self.close()

    def close(self):
        if isinstance(self.connection, Connection) and self.connection.connected:
            self.connection.release()


class RpcServer(ConsumerMixin):
    """Rpc server, used to handle client request.

    :param connection: It will be used connection with MQ server
    :param app_name: It will be used as routing_key that client send to, It also identify this app.
    :param: app_id: Every app have one unique id, It will be used identify this app
    :param sys_name: It's will be use a exchange name of MQ  default JMS

    """

    def __init__(self, connection, app_name, app_id=None, sys_name='JMS'):
        #: Use __rpc__ and app to tag rpc queue, __event__ and app for event queue
        self.connection = connection
        self.app_name = app_name
        self.app_id = app_id or uuid()
        self.sys_name = sys_name

        self.queue_name = '%s_rpc_srv_%s' % (self.sys_name, self.app_name)
        self.binding_key = 'rpc.srv.%s' % (self.app_name,)

        self.exchange = Exchange(sys_name, type='topic', durable=True)

        self.queue = Queue(self.queue_name,
                           exchange=self.exchange,
                           routing_key=self.binding_key,
                           durable=True,
                        )
        #: Key is `str`, client will call this name to process request, value is the func object
        #: HaHa _ is just for fun and pycharm get it meaning.
        self.callback_map = {'_': lambda x: {'msg': 'success', 'failed': True}, }

    def declare(self):
        channel = self.connection.channel()
        self.exchange.maybe_bind(channel)
        self.exchange.declare()
        self.queue.maybe_bind(channel)
        self.queue.declare()
        print("Declare exchange and queue ...")

    def response(self, result, message):
        props = message.properties
        print('start response')
        print(props.get('reply_to'))
        print(props.get('correlation_id'))
        print(result)
        with producers[self.connection].acquire(block=True) as producer:
            producer.publish(result,
                             serializer='json',
                             compression='bzip2',
                             exchange=self.exchange,
                             routing_key=props.get('reply_to'),
                             correlation_id=props.get('correlation_id'))
        print('end response')

    def on_message(self, body, message):
        logging.info('Start process message: %s' % (body,))
        result = self._process_request(body)
        self.response(result, message)
        message.ack()

    def get_consumers(self, Consumer, channel):
        return [
            Consumer([self.queue], callbacks=[self.on_message], accept=['json'], no_ack=False),
        ]

    def run(self, _tokens=1, **kwargs):
        """Serve for waiting request, process and response it.

        . Declare exchange
        . Declare queue
        . Binding key to queue
        . Consume it
        """
        self.declare()

        super(RpcServer, self).run(_tokens)

    def _process_request(self, body):
        print(body)
        try:
            request = json.loads(body)
        except TypeError:
            logging.error('Request <%s> unserialized failed' % (body,))
            response = {'error': 'Request <%s> unserialized failed', 'failed': True}
        else:
            func = request.get('action', None)
            if func not in self.callback_map:
                print(func)
                print(self.callback_map)
                logging.error('Request action %s was not register by server.' % (func,))
                response = {'error': 'Request action <%s> was not register by server.' % func,
                            'failed': True}
            else:
                try:
                    response = self.callback_map.get(func, None)(request.get('params', {}))
                except TypeError as e:
                    logging.error('Call function failed: %s' % (e,))
                    response = {'error': 'Call function failed: %s' % (e,), 'failed': True}

            try:
                response = json.dumps(response)
            except TypeError:
                logging.error('Function <%s> return cannot be serialize as json' % (func,))
                response = {'error': 'Function <%s> return cannot be serialize as json' % (func,),
                            'failed': True}
        return response

    def add_callback_map(self, func, name=None):
        print(func)
        print(name)
        if name is None:
            name = func.__name__

        if not isinstance(name, str):
            name = str(name)

        self.callback_map[name] = func
        print(self.callback_map)

    def callback(self, name=None, methods=None):
        """A decorator that is used to register a function for callback with the gaven name.
        This does the same thing as :meth:`_add_callback_map`
        but is intended for decorator usage:

        @rpc_server.callback('asset_add', method=['GET', 'POST'])
        def asset_add():
            pass

        :param name: callback map key, client will transfer this key to call function
        :param methods: It will be convert to part of name, for example:
                        @callback('asset_list', methods=['GET', 'POST']
                        def asset_list(message, raw):
                            pass

                        It will be append callback_map like: {
                            'asset_list.get': asset_list,
                            'asset_list.post': asset_list,
                        }
        """

        def decorator(func):
            name_ = name or func.__name__
            methods_ = methods or ['GET']

            for method in methods_:
                self.add_callback_map(func, '%s.%s' % (str(name_), method.lower()))
            return func
        return decorator


class RpcClient(object):
    """Rpc client class for app request other module.

    :param app_name: It's used as a part of client queue name
    :param app_id: Identify this app, must be unique
    :param sys_name: It's will be use a exchange name of MQ and a part of queue name, default JMS

    queue_name: {:sys_name:}_rpc_cli_{:app_name:}_{:app_id:}
    """

    def __init__(self, connection, app_name=None, app_id=None, sys_name='JMS'):

        self.connection = connection
        self.app_name = app_name
        self.app_id = app_id or uuid()
        self.sys_name = sys_name

        self.queue_name = '%s_rpc_cli_%s_%s' % (self.sys_name, self.app_name, self.app_id)
        self.binding_key = 'rpc.cli.%s.%s' % (self.app_name, self.app_id)
        self.exchange = Exchange(name=self.sys_name, type='topic')
        self.queue = Queue(name=self.queue_name,
                           exchange=self.exchange,
                           routing_key=self.binding_key)
        self.response = {}

        self.declare()

    def declare(self):
        channel = self.connection.channel()
        self.exchange.maybe_bind(channel)
        self.queue.maybe_bind(channel)

    def send_rpc(self, request_app=None, action=None, params=None, timeout=3):
        """Make a request and send to mq.

        Use params to create a request, request is a dict and can be serialized as json,
        example {'app': 'cmdb', 'action': 'asset_add', 'params': {'id': 123, 'name': 'localhost', ...}

        :param request_app: which app will be process this request, RPC server bind this
        :param action: It's a function will be call for remote app
        :param params: It's the params will be used by function
        :param timeout: waiting for response timeout timer

        """
        if request_app is None:
            logging.error('Param `request_app` should be passed')
            return {'error': 'Param `request_app` should be  passed'}

        if action is None:
            logging.error('Param `action` should be a passed')
            return {'error': 'Param `action` should be a passed'}

        if params is None:
            params = {}

        if not isinstance(params, dict):
            logging.error('Param `params` should be a dict')
            return {'error': 'Param `request` should be a dict'}

        request = {
            'from': '%s.%s' % (self.app_name, self.app_id),
            'to': request_app,
            'action': action,
            'params': params,
        }

        try:
            request = json.dumps(request)
        except TypeError:
            logging.error('`request` should be serialize failed')
            return {'error': '`request` should be serialize failed'}

        logging.info('Send request to: %s call action: %s params: %s' % (request_app, action, params))

        with producers[self.connection].acquire(block=True) as producer:
            corr_id = uuid()

            #: Publish request to MQ exchange, routing key is __rpc__ + app the server binding
            producer.publish(request,
                             exchange=self.exchange,
                             routing_key='rpc.srv.%s' % (request_app,),
                             serializer='json',
                             reply_to=self.binding_key,
                             correlation_id=corr_id,
                             )

        def on_response(body, message):
            if message.properties['correlation_id'] == corr_id:
                self.response[corr_id] = message.payload

        with Consumer(self.connection,
                      callbacks=[on_response],
                      queues=self.queue,
                      no_ack=True):

            while self.response.get(corr_id, None) is None :
                try:
                    self.connection.drain_events(timeout=timeout)
                except socket.timeout:
                    logging.error('Request timeout')
                    break

            rv = self.response.pop(corr_id, 'timeout')
            logging.info('Request <%s> get response %s.' % (corr_id, rv,))
        return rv

    def close(self):
        self.connection.close()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    app = Synapse('cmdb')

    @app.rpc_server.callback('asset_list', methods=['GET'])
    def asset_list(arg):
        return {'msg': {'name': 'asset_list',
                        'assets': [{'id': 1, 'ip': '172.16.1.2', 'assst_name': 'localhost'},
                                   {'id': 2, 'ip': '172.16.1.3', 'asset_name': 'localhost'},
                                   ]
                        },
                'failed': False}

    t = threading.Thread(target=app.run, args=(1,))
    t.daemon = True
    t.start()

    # t2 = threading.Thread(target=event_server.serve, args=())
    # t2.daemon = True
    # t2.start()
    #
    print('*' * 100)

    threads = []
    for i in range(1, 2):
        t = threading.Thread(target=app.send_rpc, args=('cmdb', 'asset_list', {}))
        t.start()
        threads.append(t)

    for i in threads:
        i.join()

    print('*' * 100)
    #
    # event_client = EventClient()
    # threads = []
    # for i in range(1, 5):
    #     t = threading.Thread(target=event_client.call, args=('cmdb', 'asset_list', {}))
    #     t.start()
    #     threads.append(t)
    #
    # for i in threads:
    #     i.join()
    #
    # time.sleep(5)
