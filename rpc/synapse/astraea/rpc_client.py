import json
import pika
import time


class RpcClient:
    _response_cache = {}

    def __init__(self, synapse):
        self._synapse = synapse
        self._channel = synapse.create_channel(desc="RpcClient")
        self._queue_name = "%s_%s_client_%s" % (synapse.sys_name, synapse.app_name, synapse.app_id)
        self._router = "client.%s.%s" % (synapse.app_name, synapse.app_id)
        synapse.log("Rpc Client Ready")

    def _check_and_create_queue(self):
        self._channel.queue_declare(queue=self._queue_name, durable=True, auto_delete=True)
        self._channel.queue_bind(queue=self._queue_name, exchange=self._synapse.sys_name, routing_key=self._router)

    def run(self):
        self._check_and_create_queue()

        def handler(ch, deliver, props, body):
            if self._synapse.debug:
                self._synapse.log("Rpc Response: (%s)%s@%s->%s %s" % (
                    props.correlation_id, props.type, props.reply_to, self._synapse.app_name, str(body)),
                                  self._synapse.LogDebug)
            self._response_cache[props.correlation_id] = json.loads(body)

        self._channel.basic_consume(consumer_callback=handler, queue=self._queue_name, no_ack=True)
        self._channel.start_consuming()

    def send(self, app, method, params):
        props = pika.BasicProperties(
            app_id=self._synapse.app_id,
            message_id=self._synapse.random_string(),
            reply_to=self._synapse.app_name,
            type=method
        )
        router = "server.%s" % app
        body = json.dumps(params)
        self._channel.basic_publish(exchange=self._synapse.sys_name, routing_key=router, properties=props, body=body)
        if self._synapse.debug:
            self._synapse.log("Rpc Request: %s->%s@%s %s" % (self._synapse.app_name, method, app, body),
                              self._synapse.LogDebug)
        ts = int(time.time())
        while True:
            if int(time.time()) - ts > self._synapse.rpc_timeout:
                res = {"rpc_error": "timeout"}
                break
            if props.message_id in self._response_cache:
                res = self._response_cache[props.message_id]
                break
        return res
