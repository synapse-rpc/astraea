# coding=utf-8
import uuid,pika,json
from .base import Base


class RpcClient(Base):
    @classmethod
    def rpc_client_queue(self):
        self.mqch.queue_declare(queue=self.sys_name+"_rpc_cli_" + self.app_name, durable=True,auto_delete=True)
        self.mqch.basic_consume(self.on_response, no_ack=True,queue=self.sys_name+"_rpc_cli_" + self.app_name)

    @classmethod
    def send_rpc(self,appname,action,params):
        print("Send A RPC REQUEST")
        self.response = None
        self.corr_id = str(uuid.uuid4())
        data = {
            "from": self.app_name,
            "to" : appname,
            "action": action,
            "params": params
        }
        self.mqch.basic_publish(exchange=self.sys_name,
                                routing_key="rpc.srv."+action,
                                   properties=pika.BasicProperties(
                                         reply_to = "rpc.cli."+self.app_name,
                                         correlation_id = self.corr_id,
                                         ),
                                   body=json.dumps(data))
        self.rpc_client_queue()
        while self.response is None:
            self.conn.process_data_events()
        return self.response.decode()["params"]

    @classmethod
    def on_response(self, ch, method, props, body):
        print(body)
        if self.corr_id == props.correlation_id:
            print(body)
            self.response = body
