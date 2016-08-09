from .base import Base
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
            event_info = {"correlation_id": str(uuid.uuid4())}
            self.conn.Producer().publish(body=data, routing_key="event.%s.%s" % (self.app_name, action),
                                         exchange=self.mqex, **event_info)
            if self.debug:
                self.log("[Synapse Debug] Publish Event: %s.%s %s" % (self.app_name, action, data))
