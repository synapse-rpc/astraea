from .synapse import Synapse
from kombu import Queue, Consumer
import time


class RpcClient(Synapse):
    rpc_client_channel = None

    def rpc_client_queue(self):
        return Queue("%s_%s_client_%s" % (self.sys_name, self.app_name, self.app_id), exchange=self.mqex,
                     routing_key="client.%s.%s" % (self.app_name, self.app_id), channel=self.rpc_client_channel,
                     auto_delete=True)

    def rpc_client_serve(self):
        self.rpc_client_channel = self.create_channel(0, "RpcClient")
        consumer = Consumer(self.conn, self.rpc_client_queue())
        consumer.register_callback(self.rpc_client_callback)
        consumer.consume()
        self.log("Rpc Client Timeout: %ds" % self.rpc_timeout)
        self.log("Rpc Client Ready")

    def rpc_client_callback(self, body, message):
        if self.debug:
            self.log("RPC Response: (%s)%s@%s->%s %s" % (
                message.properties["correlation_id"], message.properties["type"], message.properties["reply_to"],
                self.app_name, body), self.LogDebug)
        self.rpc_cli_results[message.properties["correlation_id"]] = body
        message.ack()

    def send_rpc(self, app_name, action, params):
        if self.disable_event_client:
            ret = {"rpc_error": "rpc client disabled"}
            self.log("Rpc Client Disabled: DisableRpcClient set true", self.LogError)
        else:
            props = {"app_id": self.app_id, "message_id": self.random_str(), "reply_to": self.app_name,
                     "type": action}
            self.rpc_client_channel.Producer().publish(body=params, routing_key="server.%s" % app_name,
                                                       exchange=self.mqex,
                                                       **props)
            if self.debug:
                self.log(
                    "RPC Request: (%s)%s->%s@%s %s" % (props["message_id"], self.app_name, action, app_name, params),
                    self.LogDebug)
            ts = int(time.time())
            while True:
                if int(time.time()) - ts > self.rpc_timeout:
                    ret = {"rpc_error": "timeout"}
                    break
                if props["message_id"] in self.rpc_cli_results.keys():
                    ret = self.rpc_cli_results[props["message_id"]]
                    del self.rpc_cli_results[props["message_id"]]
                    break
        return ret
