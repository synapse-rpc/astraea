from .synapse import Synapse
from kombu import Queue, Consumer
import threading


class RpcServer(Synapse):
    def rpc_server_queue(self, channel):
        queue = Queue("%s_%s_server" % (self.sys_name, self.app_name), exchange=self.mqex,
                      routing_key="server.%s" % self.app_name, durable=True, channel=channel, auto_delete=True)
        queue.declare()
        return [queue]

    def rpc_server_callback(self, body, message):
        threading.Thread(target=self.rpc_server_callback_handler, args=(body, message)).start()

    def rpc_server_callback_handler(self, body, message):
        if self.debug:
            self.log("RPC Receive: (%s)%s->%s@%s %s" % (
                message.properties['message_id'], message.properties["reply_to"], message.properties["type"],
                self.app_name,
                body), self.LogDebug)
        if message.properties["type"] in self.rpc_callback.keys():
            result = self.rpc_callback[message.properties["type"]](body, message)
        else:
            result = {"rpc_error": "method not found"}
        props = {"app_id": self.app_id, "message_id": self.random_str(), "reply_to": self.app_name,
                 "type": message.properties["type"], "correlation_id": message.properties["message_id"]}
        self.conn.Producer().publish(body=result, routing_key="client.%s.%s" % (
            message.properties["reply_to"], message.properties["app_id"]),
                                     exchange=self.mqex, **props)
        if self.debug:
            self.log("Rpc Return: (%s)%s@%s->%s %s" % (
                message.properties["message_id"], message.properties["type"], self.app_name,
                message.properties["reply_to"], result), self.LogDebug)
        message.ack()

    def rpc_server_serve(self):
        channel = self.create_channel(self.rpc_process_num, "RpcServer")
        consumer = Consumer(self.conn, self.rpc_server_queue(channel))
        consumer.register_callback(self.rpc_server_callback)
        consumer.consume()
