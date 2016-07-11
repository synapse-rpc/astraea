# coding=utf-8
import uuid,pika,json
from .base import Base


class RpcClient(Base):
    @classmethod
    def rpc_client_queue(self):
        self.mqch.queue_declare(queue=self.sys_name+"_rpc_cli_" + self.app_name, durable=True,auto_delete=True)
        self.mqch.queue_bind(exchange=self.sys_name,queue=self.sys_name+"_rpc_cli_" + self.app_name,routing_key="rpc.cli."+self.app_name)
        self.mqch.basic_consume(self.on_response, no_ack=True,queue=self.sys_name+"_rpc_cli_" + self.app_name)
        self.rpc_cli_results = {"test":"sdfsdlkfjlsdjfsdjflk"}

    @classmethod
    def send_rpc(self,app_name,action,params):
        print("Send A RPC REQUEST: %s %s %s" % (app_name,action,params))
        self.corr_id = str(uuid.uuid4())
        data = {
            "from": self.app_name,
            "to" : app_name,
            "action": action,
            "params": params
        }
        self.mqch.basic_publish(exchange=self.sys_name,
                                routing_key="rpc.srv."+app_name,
                                   properties=pika.BasicProperties(
                                         reply_to = "rpc.cli."+self.app_name,
                                         correlation_id = self.corr_id,
                                         ),
                                   body=json.dumps(data))

        # while self.corr_id not in self.rpc_cli_results.keys():
        #     print(self.rpc_cli_results.keys())
        #     self.conn.process_data_events()
        data = self.rpc_cli_results.keys()
        print(data)
        return self.rpc_cli_results["test"]

    @classmethod
    def on_response(self, ch, method, props, body):
        self.rpc_cli_results[props.correlation_id] = body
        self.rpc_cli_results["test"] ="123123123123"
        print(self.rpc_cli_results)
