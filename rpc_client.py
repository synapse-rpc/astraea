from .base import Base
from kombu import Producer, Queue, Consumer
import uuid


class RpcClient(Base):
    def rpc_client_queue(self):
        return Queue(self.sys_name + "_rpc_cli_" + self.app_name + "_" + self.app_id, exchange=self.mqex,
                     routing_key="rpc.cli." + self.app_name + "." + self.app_id, auto_delete=True)

    def rpc_client_serve(self):
        consumer = Consumer(self.conn, self.rpc_client_queue(),
                            tag_prefix="%s.%s.rpc.cli.%s" % (self.sys_name, self.app_name, self.app_id))
        consumer.register_callback(self.rpc_client_callback)
        consumer.qos(prefetch_count=self.proccess_num, prefetch_size=0, apply_global=False)
        consumer.consume()
        self.log("[Synapse Info] Rpc Client Handler Listening")

    def rpc_client_callback(self, body, message):
        self.rpc_cli_results[message.properties["correlation_id"]] = body["params"]

    def send_rpc(self, app_name, action, params):
        if self.disable_event_client:
            self.log("[Synapse Error] Event Send Not Success: DisableEventClient set true")
        else:
            corr_id = str(uuid.uuid4())
            data = {
                "from": self.app_name + "." + self.app_id,
                "to": "event",
                "action": action,
                "params": params
            }
            properties = {"correlation_id": corr_id, "reply_to": "rpc.cli." + self.app_name + "." + self.app_id}
            producer = Producer(self.conn, self.mqex, "rpc.srv." + app_name)
            producer.publish(data, **properties)
            if self.debug:
                self.log("Send A RPC REQUEST: %s %s %s" % (app_name, action, params))
            while corr_id in self.rpc_cli_results.keys():
                ret = self.rpc_cli_results[corr_id]
                del self.rpc_cli_results[corr_id]
                return ret