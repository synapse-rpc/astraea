import json
import pika


class EventClient:
    def __init__(self, synapse):
        self._synapse = synapse
        self.ch = synapse.create_channel(desc="EventClient")
        synapse.log("Event Client Ready")

    def send(self, event, params):
        props = pika.BasicProperties(
            app_id=self._synapse.app_id,
            message_id=self._synapse.random_string(),
            reply_to=self._synapse.app_name,
            type=event
        )
        router = "event.%s.%s" % (self._synapse.app_name, event)
        body = json.dumps(params)
        self.ch.basic_publish(exchange=self._synapse.sys_name, routing_key=router, properties=props, body=body)
        if self._synapse.debug:
            self._synapse.log("Event Publish: %s@%s %s" % (event, self._synapse.app_name, body), self._synapse.LogDebug)
