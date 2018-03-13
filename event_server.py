from .synapse import Synapse
from kombu import Queue, Consumer
import threading
import json


class EventServer(Synapse):
    def event_server_queue(self, channel):
        queue = Queue("%s_%s_event" % (self.sys_name, self.app_name), channel=channel, durable=True, auto_delete=True)
        queue.declare()
        for k in self.event_callback.keys():
            queue.bind_to(exchange=self.sys_name, routing_key="event.%s" % k)
        return [queue]

    def event_server_callback(self, body, message):
        threading.Thread(target=self.event_server_callback_handler, args=(body, message)).start()

    def event_server_callback_handler(self, body, message):
        if self.debug:
            self.log(
                "Event Receive: %s@%s %s" % (message.properties['type'], message.properties['reply_to'], body),
                self.LogDebug)
        if self.event_callback[message.delivery_info['routing_key'][6:]](json.loads(body), message):
            message.ack()
        else:
            message.reject(requeue=True)

    def event_server_serve(self):
        channel = self.create_channel(self.event_process_num, "EventServer")
        consumer = Consumer(channel, self.event_server_queue(channel))
        consumer.register_callback(self.event_server_callback)
        consumer.consume()
