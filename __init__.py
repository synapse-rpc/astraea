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
import sys
import json
import logging
import time


class RpcBase(object):
    """Rpc base class for inherit.

    Make connection with MQ server.

    :param mq_host: Nothing to say, it's so obvious
    :param mq_port: Look upside
    :param mq_user: Look upside
    :param mq_pass: Look upside
    """

    def __init__(self, mq_host='localhost', mq_port=5672, mq_user=None, mq_pass=None):
        self.mq_host = mq_host
        self.mq_port = mq_port
        self.mq_user = mq_user
        self.mq_pass = mq_pass
        # make a connection with MQ server
        self.connection = self.make_connection()
        self.channel = self.get_channel()
        self.response = {}

    def make_connection(self):
        if self.mq_user is None:
            self.connection = rv = pika.BlockingConnection(pika.ConnectionParameters(
                host=self.mq_host, port=self.mq_port))
        else:
            self.connection = rv = pika.BlockingConnection(pika.ConnectionParameters(
                    host=self.mq_host, port=self.mq_port,
                    credentials=pika.PlainCredentials(username=self.mq_user,
                                                      password=self.mq_pass)
                    ))
        return rv

    def get_channel(self):
        self.channel = rv = self.connection.channel()
        return rv


class RpcClient(RpcBase):
    """Rpc client class for app request other module.

    :param mq_host: Nothing to say, it's so obvious
    :param mq_port: Look upside
    :param mq_user: Look upside
    :param mq_pass: Look upside
    :param client_name: It's have little affect except for logging
    :param sys_name: It's will be use a exchange name of MQ  default JMS
    """

    def __init__(self, client_name=None, sys_name=None, **kwargs):
        super(RpcClient, self).__init__(**kwargs)

        if client_name is None:
            self.client_name = str(uuid.uuid4())
        else:
            self.client_name = client_name

        if sys_name is None:
            self.sys_name = 'JMS'
        else:
            self.sys_name = sys_name

    def call(self, app=None, action=None, params=None, timeout=3):
        """Make a request and send to mq.

        Use params to create a request, request is a dict and can be serialized as json,
        example {'app': 'cmdb', 'action': 'asset_add', 'params': {'id': 123, 'name': 'localhost', ...}

        :param app: which app will be process this request, RPC server bind this
        :param action: It's a function will be call for remote app
        :param params: It's the params will be used by function
        :param timeout: waiting for response timeout timer

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

        #: Publish request to MQ exchange, routing key is __rpc__ + app the server binding
        self.channel.basic_publish(exchange=self.sys_name,
                                   routing_key='__rpc__' + app,
                                   properties=pika.BasicProperties(
                                       reply_to=callback_queue,
                                       correlation_id=corr_id
                                   ),
                                   body=request)

        start = end = time.time()
        while self.response.get(corr_id, None) is None and end - start < timeout:
            self.connection.process_data_events(time_limit=1)
            end = time.time()
        rv = self.response.pop(corr_id)
        logging.info('Request <%s> get response %s.' % (corr_id, rv,))
        return rv

    def close(self):
        self.connection.close()


class RpcServer(RpcBase):
    """Rpc server, used to handle client request.

    :param app: It will be used as routing_key that client send to, It also identify this app.
    :param sys_name: :param sys_name: It's will be use a exchange name of MQ  default JMS

    """

    def __init__(self, app=None, sys_name=None, **kwargs):
        super(RpcServer, self).__init__(**kwargs)

        if app is None:
            logging.error('Param `app` should be pass')
            sys.exit(1)
        else:
            self.app = app

        #: Use __rpc__ and app to tag rpc queue, __event__ and app for event queue
        self.queue = '__rpc__' + self.app

        if sys_name is None:
            self.sys_name = 'JMS'
        else:
            self.sys_name = sys_name

        #: Key is `str`, client will call this name to process request, value is the func object
        #: HaHa _ is just for fun and pycharm get it mean.
        self.callback_map = {'_': lambda x: {'msg': 'success', 'failed': True},}

    def serve(self):
        """Serve for waiting request, process and response it.

        . Declare exchange
        . Declare queue
        . Binding key to queue
        . Consume it
        """
        self.channel.exchange_declare(exchange=self.sys_name, exchange_type='topic')
        self.channel.queue_declare(queue=self.queue, durable=True, auto_delete=True)
        self.channel.queue_bind(exchange=self.sys_name, queue=self.queue, routing_key=self.queue)

        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(self._on_request, queue=self.queue)

        print(' [x] Awaiting RPC requests')
        self.channel.start_consuming()

    def _process_request(self, body):
        try:
            request = json.loads(body)
        except TypeError:
            logging.error('Request <%s> unserialized failed' % (body,))
            response = {'error': 'Request <%s> unserialized failed', 'failed': True}
        else:
            func = request.get('action', None)
            if func not in self.callback_map:
                logging.error('Request action <%s> was not register by server.' % (func,))
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

    def _on_request(self, ch, method, props, body):
        logging.info('Start process request: <%s>' % (body,))
        response = self._process_request(body)
        self.channel.basic_publish(exchange=self.sys_name,
                                   routing_key=props.reply_to,
                                   properties=pika.BasicProperties(
                                       correlation_id=props.correlation_id),
                                   body=response)
        self.channel.basic_ack(delivery_tag=method.delivery_tag)

    def add_callback_map(self, func, name=None):
        if name is None:
            name = func.__name__

        if not isinstance(name, str):
            name = str(name)

        self.callback_map[name] = func

    def callback(self, name=None):
        """A decorator that is used to register a function for callback with the gaven name.
        This does the same thing as :meth:`_add_callback_map`
        but is intended for decorator usage:

        @rpc_server.callback('asset_add')
        def asset_add():
            pass

        :param name: callback map key, client will transfer this key to call function
        """
        def decorator(func):
            self.add_callback_map(func, name)
            return func
        return decorator


class EventClient(RpcClient):
    """Event client most likely Rpc client, except Event client needless to get response.

    So I decide inherit from :class: `RpcClient` .
    """
    def __init__(self, **kwargs):
        super(EventClient, self).__init__(**kwargs)

    def call(self, app=None, action=None, params=None, timeout=3):
        """Make a request and send to mq.

        Use params to create a request, request is a dict and can be serialized as json, like rpc client
        example {'app': 'cmdb', 'action': 'asset_add', 'params': {'id': 123, 'name': 'localhost', ...}

        :param app: which app will be get this event, event server bind this
        :param action: It's a function will be call for remote app
        :param params: It's the params will be used by function
        :param timeout: None of business for event client
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

        logging.info('Send event to: %s call action: %s params: %s' % (app, action, params))

        #: Publish request to MQ exchange, routing key is __rpc__ + app the server binding
        self.channel.basic_publish(exchange=self.sys_name,
                                   routing_key='__event__' + app,
                                   body=request)
        logging.info('Send event finished .')
        return {'msg': 'Send event finished'}


class EventServer(RpcServer):
    """Event server most like RpcServer, but difference more, I decide rewrite it.

    """
    def __init__(self, **kwargs):
        super(EventServer, self).__init__(**kwargs)

        self.queue = '__event__' + self.app

    def _on_request(self, ch, method, props, body):
        """Event server needn't return any response, just process it and confirm get it.
        """

        self._process_request(body)
        self.channel.basic_ack(delivery_tag=method.delivery_tag)
        logging.info('Finish process event: <%s>' % (body,))


if __name__ == '__main__':
    import threading
    logging.basicConfig(level=logging.INFO)
    rpc_server = RpcServer(app='cmdb')
    event_server = EventServer(app='cmdb')

    @rpc_server.callback('asset_list')
    @event_server.callback('asset_list')
    def asset_list(arg):
        return {'msg': {'name': 'asset_list',
                        'assets': [{'id': 1, 'ip': '172.16.1.2', 'assst_name': 'localhost'},
                                   {'id': 2, 'ip': '172.16.1.3', 'asset_name': 'localhost'},
                                   ]
                        },
                'failed': False}

    t = threading.Thread(target=rpc_server.serve, args=())
    t.daemon = True
    t.start()

    t2 = threading.Thread(target=event_server.serve, args=())
    t2.daemon = True
    t2.start()

    print('*' * 100)

    rpc_client = RpcClient()
    threads = []
    for i in range(1, 5):
        t = threading.Thread(target=rpc_client.call, args=('cmdb', 'asset_list', {}))
        t.start()
        threads.append(t)

    for i in threads:
        i.join()

    print('*' * 100)

    event_client = EventClient()
    threads = []
    for i in range(1, 5):
        t = threading.Thread(target=event_client.call, args=('cmdb', 'asset_list', {}))
        t.start()
        threads.append(t)

    for i in threads:
        i.join()

    time.sleep(5)
