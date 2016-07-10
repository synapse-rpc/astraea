# coding=utf-8
import uuid,pika,json
from .base import Base

class EventClient(Base):
    @classmethod
    def send_event(self,action,params):
        if self.disable_event_client:
            self.log("[Synapse Error] Event Send Not Success: DisableEventClient set true")
        else:
            data = {
                "from": self.app_name,
                "to" : "event",
                "action": action,
                "params": params
            }
            self.mqch.basic_publish(exchange=self.sys_name,
                                    routing_key="event."+self.app_name+"."+action,
                                    properties=pika.BasicProperties(
                                            correlation_id = str(uuid.uuid4()),
                                            ),
                                    body=json.dumps(data))
            if self.debug:
                    self.log("[Synapse Debug] Publish Event: %s.%s %s" % (self.app_name,action,data))