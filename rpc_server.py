import json
import pika


class RpcServer:
    def __init__(self, synapse):
        self.s = synapse
        self.ch = synapse.create_channel(synapse.rpc_process_num, "RpcServer")
        self.queue_name = "%s_%s_server" % (self.s.sys_name, self.s.app_name)
        self.router = "server.%s" % self.s.app_name

    def check_and_create_queue(self):
        self.ch.queue_declare(queue=self.queue_name, durable=True, auto_delete=True)
        self.ch.queue_bind(queue=self.queue_name, exchange=self.s.sys_name, routing_key=self.router)

    def run(self):
        self.check_and_create_queue()

        def handler(ch, deliver, props, body):
            if self.s.debug:
                self.s.log("Rpc Receive: (%s)%s->%s@%s %s" % (
                    props.message_id, props.reply_to, props.type, self.s.app_name, str(body)), self.s.LogDebug)
            if props.type in self.s.rpc_callback:
                res = self.s.rpc_callback[props.type](json.loads(body), props)
            else:
                res = {"rpc_error": "method not found"}
            body = json.dumps(res)
            reply = "client.%s.%s" % (props.reply_to, props.app_id)
            ret_props = pika.BasicProperties(
                app_id=self.s.app_id,
                correlation_id=self.s.correlation_id,
                message_id=self.s.random_string(),
                reply_to=self.s.app_name,
                type=props.type
            )
            ch.basic_publish(exchange=self.s.sys_name, routing_key=reply, properties=ret_props, body=body)
            if self.s.debug:
                self.s.log("Rpc Return: (%s)%s@%s->%s %s" % (
                    props.message_id, props.type, self.s.app_name, props.reply_to, body), self.s.LogDebug)

        self.ch.basic_consume(consumer_callback=handler, queue=self.queue_name, no_ack=True)
        self.ch.start_consuming()
