import json
import pika


class EventClient:
    def __init__(self, synapse):
        self.s = synapse
        self.ch = synapse.create_channel(desc="EventClient")
        self.s.log("Event Client Ready")

    def send(self, event, params):
        props = pika.BasicProperties(
            app_id=self.s.app_id,
            message_id=self.s.random_string(),
            reply_to=self.s.app_name,
            type=event
        )
        router = "event.%s.%s" % (self.s.app_name, event)
        body = json.dumps(params)
        self.ch.basic_publish(exchange=self.s.sys_name, routing_key=router, properties=props, body=body)
        if self.s.debug:
            self.s.log("Event Publish: %s@%s %s" % (event, self.s.app_name, body), self.LogDebug)
