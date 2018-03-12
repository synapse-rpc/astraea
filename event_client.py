from .synapse import Synapse


class EventClient(Synapse):
    event_client_channel = None

    def event_client_serve(self):
        self.event_client_channel = self.create_channel(0, "EventClient")
        self.log("Event Client Ready")

    def send_event(self, action, params):
        if self.disable_event_client:
            self.log("Event Client Disabled: DisableEventClient set true", self.LogError)
        else:
            event_info = {"app_id": self.app_id, "message_id": self.random_str(), "reply_to": self.app_name,
                          "type": action}
            self.event_client_channel.Producer().publish(body=params,
                                                         routing_key="event.%s.%s" % (self.app_name, action),
                                                         exchange=self.sys_name, **event_info)
            if self.debug:
                self.log("Event Publish: %s@%s %s" % (action, self.app_name, params), self.LogDebug)
