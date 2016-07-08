from .base import Base
import json


class EventServer(Base):
    @classmethod
    def event_exchange(self):
        self.mqch.exchange_declare(exchange='event',
                                   type='topic',
                                   durable=True
                                   )

    @classmethod
    def event_queue(self):
        self.mqch.queue_declare(queue="event_" + self.app_name, durable=True)
        for k in self.event_callback_map.keys():
            self.mqch.queue_bind(exchange='event', queue="event_" + self.app_name, routing_key=k)

    @classmethod
    def event_serve(self):
        self.event_exchange()
        self.event_queue()

        def callback(ch, method, properties, body):
            if method.exchange == "event" and method.routing_key in self.event_callback_map.keys():
                data = json.loads(body.decode())
                if self.debug:
                    self.log("[Synapse Debug] Receive Event: %s %s" % (method.routing_key, data))
                self.event_callback_map[method.routing_key](data["params"], body)

        self.mqch.basic_consume(callback,
                                queue="event_" + self.app_name,
                                no_ack=True)
        self.log('[Synapse Info] Event Handler Listening')
        self.mqch.start_consuming()
