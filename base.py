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
import uuid

from kombu import Exchange, Queue, Connection, Consumer
from kombu.mixins import ConsumerMixin
from kombu.pools import producers


def create_logger(app):
    handler = logging.StreamHandler()
    log_fmt = '%(asctime)s [%(name)s %(levelname)s] %(message)s'
    formatter = logging.Formatter(log_fmt)
    handler.setFormatter(formatter)

    logger = logging.getLogger(app.app_name)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    return logger


class Synapse(object):
    config = {
        'MQ_HOST': 'localhost',
        'MQ_PORT': 5672,
        'MQ_USER': None,
        'MQ_PASSWORD': None,
        'DEBUG': False,
        'WORKER_PROCESSES': 4,
        'WORKER_CONNECTIONS': 100,
    }

    def __init__(self, app_name, app_id=None, sys_name='jumpserver'):
        self.app_name = app_name
        self.app_id = app_id or uuid.uuid4().hex[-20:].upper()
        self.sys_name = sys_name
        self.connection = self.create_connection()
        self.rpc_server = RpcServer(self.connection, self.app_name, self.app_id, sys_name=sys_name)
        self.rpc_client = RpcClient(self.connection, self.app_name, self.app_id, sys_name=sys_name)

    @property
    def send_rpc(self):
        return self.rpc_client.send_rpc

    def send_event(self):
        pass

    @property
    def rpc_callback(self):
        return self.rpc_server.callback

    def create_connection(self):
        if self.config.get('MQ_USER', None) is None:
            amqp_uri = 'amqp://%s:%s//' % (self.config.get('MQ_HOST', 'localhost'),
                                           self.config.get('MQ_PORT', 5672))
        else:
            amqp_uri = 'amqp://%s:%s@%s:%s//' % (self.config.get('MQ_USER'),
                                                 self.config.get('MQ_PASSWORD'),
                                                 self.config.get('MQ_HOST', 'localhost'),
                                                 self.config.get('MQ_PORT', 5672))

        self.connection = rv = Connection(amqp_uri, insist=True, ssl=False)
        if rv is None:
            logging.error("Create connection will MQ server Error")
            raise SystemError('Create connection will MQ server Error')

        logging.info('System Name: %s' % self.sys_name)
        logging.info('App Name: %s' % self.sys_name)
        logging.info('App ID: %s' % self.app_id)
        logging.info('Worker Processes: %s' % self.config.get('WORKER_PROCESSES'))
        logging.info('Worker Max Connections: %s' % self.config.get('WORKER_CONNECTIONS'))
        logging.info('Rabbit MQ Connection Created')
        return rv

    def run(self, process_num=None):
        if process_num is None:
            process_num = self.config.get('WORKER_PROCESSES', None) or 1

        processes = []
        try:
            for j in range(process_num):
                process = Process(target=self.rpc_server.run, args=())
                process.start()
                processes.append(process)
                print('Awaiting RPC requests [%s] ' % (j,))

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
    :param app_id: Every app have one unique id, It will be used identify this app
    :param sys_name: It's will be use a exchange name of MQ  default JMS

    """

    def __init__(self, connection, app_name, app_id=None, sys_name='jumpserver'):
        self.connection = connection
        self.app_name = app_name
        self.app_id = app_id or uuid.uuid4().hex[-20:].upper()
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
        logging.info("Declare Exchange: %s " % (self.exchange.name,))
        logging.info("Declare Queue: %s " % (self.queue_name,))
        logging.info('Binding Queue: %s -> Exchange %s, Key: %s' % (self.binding_key,
                                                                    self.exchange.name,
                                                                    self.binding_key))

    def response(self, result, message):
        props = message.properties
        with producers[self.connection].acquire(block=True) as producer:
            producer.publish(result,
                             serializer='json',
                             compression='bzip2',
                             exchange=self.exchange,
                             routing_key=props.get('reply_to'),
                             correlation_id=props.get('correlation_id'))

    def on_message(self, body, message):
        result = self._process_request(body, message)
        self.response(result, message)
        message.ack()

    def get_consumers(self, _, channel):
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

    def _process_request(self, body, message):
        try:
            request = json.loads(body)
        except TypeError:
            logging.error('Request <%s> deserialized failed' % (body,))
            response = {'error': 'Request <%s> deserialized failed', 'failed': True}
        else:
            action = request.get('action', ())
            params = request.get('params', {})
            if action not in self.callback_map:
                logging.error('Request Action %s was not Register by RPC server.' % (action,))
                response = {'error': 'Request action <%s> was not register by server.' % (action,),
                            'failed': True}
            else:
                try:
                    response = self.callback_map.get(action, lambda x, y: x, y)(params, message)
                except TypeError as e:
                    logging.error('Call action %s failed: %s' % (action, e,))
                    response = {'error': 'Call function failed: %s' % (e,), 'failed': True}

            try:
                response = json.dumps(response)
            except TypeError:
                logging.error('Function <%s> return cannot be serialize as json' % (action,))
                response = {'error': 'Function <%s> return cannot be serialize as json' % (action,),
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

    def send_rpc(self, request_app=None, func=None, method='GET', args=None, kwargs=None, timeout=3):
        """Make a request and send to mq.

        Use params to create a request, request is a dict and can be serialized as json,

        example: self.send_rpc('cmdb', func='asset_list',
                                method='GET', args=('hello', 'world'),
                                kwargs={'asset_name': 'localhost'})

        :param request_app: which app will be process this request, RPC server bind this
        :param func: It's a function will be call for remote app
        :param method: GET, POST, HEAD etc, will pass to function
        :param args: It's will be pass into func as args
        :param kwargs: It's will be pass into func as kwargs
        :param timeout: waiting for response timeout timer

        """
        if request_app is None:
            logging.error('Param `request_app` should be passed')
            return {'error': 'Param `request_app` should be  passed'}

        request = {
            'from': '%s.%s' % (self.app_name, self.app_id),
            'to': request_app,
            'func': func,
            'method': method,
            'args': args or (),
            "kwargs": kwargs or {},
        }

        try:
            request = json.dumps(request)
        except TypeError:
            logging.error('`request` should be serialize failed')
            return {'error': '`request` should be serialize failed'}

        logging.info('Send request to: %s call %s(%s, %s, method=%s)' %
                     (request_app,
                      func,
                      ','.join(args),
                      ','.join(['%s=%s' % (k, v)
                                for k, v in kwargs.items()]),
                      method.lower(),
                      ))

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
