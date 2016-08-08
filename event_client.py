from .base import Base
from kombu import Producer
import uuid


class EventClient(Base):
    def send_event(self, action, params):
        if self.disable_event_client:
            self.log("[Synapse Error] Event Send Not Success: DisableEventClient set true")
        else:
            data = {
                "from": self.app_name + "." + self.app_id,
                "to": "event",
                "action": action,
                "params": params
            }
            if self.debug:
                self.log("[Synapse Debug] Publish Event: %s.%s %s" % (self.app_name, action, data))
            properties = {"correlation_id": str(uuid.uuid4())}
            producer = Producer(self.conn, self.mqex, "event." + self.app_name + "." + action)
            producer.publish(data, **properties)
            if self.debug:
                self.log("[Synapse Debug] Publish Event: %s.%s %s" % (self.app_name, action, data))
