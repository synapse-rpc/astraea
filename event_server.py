import json


class EventServer:
    def __init__(self, synapse):
        self.s = synapse
        self.ch = synapse.create_channel(synapse.event_process_num, "EventServer")
        self.queue_name = "%s_%s_event" % (self.s.sys_name, self.s.app_name)

    def check_and_create_queue(self):
        self.ch.queue_declare(queue=self.queue_name, durable=True, auto_delete=True)
        for k in self.s.event_callback.keys():
            self.ch.queue_bind(queue=self.queue_name, exchange=self.s.sys_name, routing_key="event.%s" % k)

    def run(self):
        self.check_and_create_queue()

        def handler(ch, deliver, props, body):
            if self.s.debug:
                self.s.log("Event Receive: %s@%s %s" % (props.type, props.reply_to, str(body)))
            key = "%s.%s" % (props.reply_to, props.type)
            if key in self.s.event_callback:
                res = self.s.event_callback[key](json.loads(body), props)
                if res:
                    ch.basic_ack(deliver.delivery_tag)
                else:
                    ch.basic_nack(deliver.delivery_tag)
            else:
                ch.basic_nack(deliver.delivery_tag, requeue=False)

        self.ch.basic_consume(consumer_callback=handler, queue=self.queue_name)
        self.ch.start_consuming()
