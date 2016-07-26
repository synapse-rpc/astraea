# coding=utf-8
from .base import Base
import json
import pika


class RpcServer(Base):
    @classmethod
    def rpc_server_queue(self):
        self.mqch.queue_declare(queue=self.sys_name + "_rpc_srv_" + self.app_name, durable=True, auto_delete=True)
        for k in self.event_callback_map.keys():
            self.mqch.queue_bind(exchange=self.sys_name, queue=self.sys_name + "_rpc_srv_" + self.app_name,
                                 routing_key="rpc.srv." + self.app_name)

    @classmethod
    def rpc_server_serve(self):
        self.rpc_server_queue()

        def callback(ch, method, properties, body):
            data = json.loads(body.decode())
            ret = {"code": 200, "message": "OK"}
            if data["action"] not in self.rpc_callback_map.keys():
                ret["code"] = 404
                ret["message"] = "The Rpc Action Not Found"
            else:
                act = data["action"]
                if self.debug:
                    self.log("[Synapse Debug] Receive Rpc Request: %s" % (data))
                ret_source = self.rpc_callback_map[act](data["params"], body)
                if "code" in ret_source.keys() and "message" not in ret_source.keys():
                    ret_source["message"] = "code (%d), no more message" % (ret_source["code"])
                for k in ret_source:
                    ret[k] = ret_source[k]
            responseJSON = json.dumps({
                "from": self.app_name + "." + self.app_id,
                "to": data["from"],
                "action": "reply-%s" % (data["action"]),
                "params": ret
            })
            ch.basic_publish(exchange=self.sys_name,
                             routing_key=properties.reply_to,
                             properties=pika.BasicProperties(correlation_id= \
                                                                 properties.correlation_id),
                             body=responseJSON)
            if self.debug:
                self.log("[Synapse Debug] Reply Rpc Request: %s" % (responseJSON))
            ch.basic_ack(delivery_tag=method.delivery_tag)

        self.mqch.basic_qos(prefetch_count=1)
        self.mqch.basic_consume(callback,
                                queue=self.sys_name + "_rpc_srv_" + self.app_name)
        self.log('[Synapse Info] Rpc Server Handler Listening')
        self.mqch.start_consuming()
