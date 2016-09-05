from .common import Common
from kombu import Queue, Consumer, Producer
import threading


class RpcServer(Common):
    def rpc_server_queue(self):
        return Queue(self.sys_name + "_rpc_srv_" + self.app_name, exchange=self.mqex,
                     routing_key="rpc.srv." + self.app_name, auto_delete=True)

    def rpc_server_callback(self, body, message):
        t = threading.Thread(target=self.rpc_server_callback_handler, args=(body, message))
        t.start()

    def rpc_server_callback_handler(self, body, message):
        if self.debug:
            self.log("[Synapse Debug] Receive Rpc Request: %s" % body)
        if body['action'] in self.rpc_callback_map.keys():
            result = self.rpc_callback_map[body['action']](body['params'], message)
        else:
            result = {"code": 404, "message": "The Rpc Action Not Found"}
        if "code" in result.keys():
            if "message" not in result.keys():
                result["message"] = "Unknow status: %d" % result["code"]
        response = {
            "from": self.app_name + "." + self.app_id,
            "to": body["from"],
            "action": "reply-%s" % body['action'],
            "params": result
        }

        properties = {'correlation_id': message.properties["correlation_id"]}
        self.conn.Producer().publish(body=response, routing_key=message.properties["reply_to"],
                                     exchange=self.mqex, **properties)
        if self.debug:
            self.log("[Synapse Debug] Reply Rpc Request: %r" % response)
        message.ack()

    def rpc_server_serve(self):
        consumer = Consumer(self.conn, self.rpc_server_queue(),
                            tag_prefix="%s.%s.rpc.srv.%s" % (self.sys_name, self.app_name, self.app_id))
        consumer.register_callback(self.rpc_server_callback)
        consumer.qos(prefetch_count=self.proccess_num, prefetch_size=0, apply_global=False)
        consumer.consume()
        self.log('[Synapse Info] Rpc Server Handler Listening')
