# coding=utf-8
from .base import Base
import json
import pika


class RpcServer(Base):
    @classmethod
    def rpc_server_queue(self):
        self.mqch.queue_declare(queue=self.sys_name+"_rpc_srv_" + self.app_name, durable=True,auto_delete=True)
        for k in self.event_callback_map.keys():
            self.mqch.queue_bind(exchange=self.sys_name, queue=self.sys_name+"_rpc_srv_" + self.app_name,routing_key="rpc.srv."+self.app_name)

    @classmethod
    def rpc_server_serve(self):
        if "*" not in self.rpc_callback_map.keys():
            self.log("[Synapse Error] Rpc Handler Must have * to handle unknow Request")
            exit
        self.rpc_server_queue()

        def callback(ch, method, properties, body):
            data = json.loads(body.decode())
            if data["action"] not in self.rpc_callback_map.keys():
                act = "*"
            else:
                act = data["action"]
            if self.debug:
                self.log("[Synapse Debug] Receive Rpc Request: %s" % (data))
            ret = self.rpc_callback_map[act](data["params"], body)
            responseJSON = json.dumps({
                "from": self.app_name,
                "to": data["from"],
                "action": "reply-%s" % (data["action"]),
                "params": ret
            })
            ch.basic_publish(exchange=self.sys_name,
                     routing_key=properties.reply_to,
                     properties=pika.BasicProperties(correlation_id = \
                                                         properties.correlation_id),
                     body=responseJSON)
            if self.debug:
                self.log("[Synapse Debug] Reply Rpc Request: %s" % (responseJSON))
            ch.basic_ack(delivery_tag = method.delivery_tag)
        self.mqch.basic_qos(prefetch_count=1)
        self.mqch.basic_consume(callback,
                                queue=self.sys_name+"_rpc_srv_" + self.app_name)
        self.log('[Synapse Info] Rpc Server Handler Listening')
        self.mqch.start_consuming()
