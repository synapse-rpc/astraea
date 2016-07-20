# coding=utf-8
import uuid, pika, json, time
from .base import Base


class RpcClient(Base):
    response = None

    @classmethod
    def send_rpc(self, app_name, action, params):
        print("Send A RPC REQUEST: %s %s %s" % (app_name, action, params))
        self.mqch.queue_declare(queue=self.sys_name + "_rpc_cli_" + self.app_name + "_" + self.app_id, durable=True,
                                auto_delete=True)
        self.mqch.queue_bind(exchange=self.sys_name,
                             queue=self.sys_name + "_rpc_cli_" + self.app_name + "_" + self.app_id,
                             routing_key="rpc.cli." + self.app_name + "." + self.app_id)
        self.mqch.basic_consume(self.on_response, no_ack=True,
                                queue=self.sys_name + "_rpc_cli_" + self.app_name + "_" + self.app_id)

        self.response = None
        self.corr_id = str(uuid.uuid4())
        data = {
            "from": self.app_name + "." + self.app_id,
            "to": app_name,
            "action": action,
            "params": params
        }
        self.mqch.basic_publish(exchange=self.sys_name,
                                routing_key="rpc.srv." + app_name,
                                properties=pika.BasicProperties(
                                    reply_to="rpc.cli." + self.app_name + "." + self.app_id,
                                    correlation_id=self.corr_id,
                                ),
                                body=json.dumps(data))
        while self.response is None:
            self.conn.process_data_events()
        return self.response

    @classmethod
    def on_response(self, ch, method, props, body):
        if self.corr_id == props.correlation_id:
            self.response = body
