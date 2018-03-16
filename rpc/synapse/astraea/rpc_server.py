import json
import pika


class RpcServer:
    def __init__(self, synapse):
        self._synapse = synapse
        self._channel = synapse.create_channel(process_num=synapse.rpc_process_num, desc="RpcServer")
        self._queue_name = "%s_%s_server" % (self._synapse.sys_name, self._synapse.app_name)
        self._router = "server.%s" % self._synapse.app_name

    def _check_and_create_queue(self):
        self._channel.queue_declare(queue=self._queue_name, durable=True, auto_delete=True)
        self._channel.queue_bind(queue=self._queue_name, exchange=self._synapse.sys_name, routing_key=self._router)

    def run(self):
        self._check_and_create_queue()

        def handler(ch, deliver, props, body):
            if self._synapse.debug:
                self._synapse.log("Rpc Receive: (%s)%s->%s@%s %s" % (
                    props.message_id, props.reply_to, props.type, self._synapse.app_name, str(body)),
                                  self._synapse.LogDebug)
            if props.type in self._synapse.rpc_callback:
                res = self._synapse.rpc_callback[props.type](json.loads(body), props)
            else:
                res = {"rpc_error": "method not found"}
            body = json.dumps(res)
            reply = "client.%s.%s" % (props.reply_to, props.app_id)
            ret_props = pika.BasicProperties(
                app_id=self._synapse.app_id,
                correlation_id=props.message_id,
                message_id=self._synapse.random_string(),
                reply_to=self._synapse.app_name,
                type=props.type
            )
            ch.basic_publish(exchange=self._synapse.sys_name, routing_key=reply, properties=ret_props, body=body)
            if self._synapse.debug:
                self._synapse.log("Rpc Return: (%s)%s@%s->%s %s" % (
                    props.message_id, props.type, self._synapse.app_name, props.reply_to, body), self._synapse.LogDebug)

        self._channel.basic_consume(consumer_callback=handler, queue=self._queue_name, no_ack=True)
        self._channel.start_consuming()
