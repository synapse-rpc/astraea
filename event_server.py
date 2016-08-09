from .base import Base
from kombu import Queue, Consumer
import threading


class EventServer(Base):
    def event_server_queue(self):
        queues = []
        for k in self.event_callback_map.keys():
            queues.append(
                Queue(self.sys_name + "_event_" + self.app_name, exchange=self.mqex, routing_key="event." + k))
        return queues

    def event_server_callback(self, body, message):
        t = threading.Thread(target=self.event_server_callback_handler, args=(body, message))
        t.start()

    def event_server_callback_handler(self, body, message):
        if self.debug:
            self.log("[Synapse Debug] Receive Event: %s %s" % (message.delivery_info['routing_key'][6:], body))
        if self.event_callback_map[message.delivery_info['routing_key'][6:]](body['params'], message):
            message.ack()
        else:
            message.reject(requeue=True)

    def event_server_serve(self):
        consumer = Consumer(self.conn, self.event_server_queue(),
                            tag_prefix="%s.%s.event.%s" % (self.sys_name, self.app_name, self.app_id))
        consumer.register_callback(self.event_server_callback)
        consumer.qos(prefetch_count=self.proccess_num, prefetch_size=0, apply_global=False)
        consumer.consume()
        self.log('[Synapse Info] Event Server Handler Listening')
