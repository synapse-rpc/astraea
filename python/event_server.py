from .base import Base
import json


class EventServer(Base):
    @classmethod
    def event_server_queue(self):
        self.mqch.queue_declare(queue=self.sys_name+"_event_" + self.app_name, durable=True)
        for k in self.event_callback_map.keys():
            self.mqch.queue_bind(exchange=self.sys_name, queue=self.sys_name+"_event_" + self.app_name, routing_key="event."+k)

    @classmethod
    def event_server_serve(self):
        self.event_server_queue()

        def callback(ch, method, properties, body):
            if method.exchange == self.sys_name and method.routing_key[6:] in self.event_callback_map.keys():
                data = json.loads(body.decode())
                if self.debug:
                    self.log("[Synapse Debug] Receive Event: %s %s" % (method.routing_key, data))
                self.event_callback_map[method.routing_key[6:]](data["params"], body)

        self.mqch.basic_consume(callback,
                                queue=self.sys_name+"_event_" + self.app_name,
                                no_ack=True)
        self.log('[Synapse Info] Event Server Handler Listening')
        self.mqch.start_consuming()
