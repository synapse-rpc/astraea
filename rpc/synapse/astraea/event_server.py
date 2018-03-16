import json


class EventServer:
    def __init__(self, synapse):
        self._synapse = synapse
        self._channel = synapse.create_channel(process_num=synapse.event_process_num, desc="EventServer")
        self._queue_name = "%s_%s_event" % (self._synapse.sys_name, self._synapse.app_name)

    def _check_and_create_queue(self):
        self._channel.queue_declare(queue=self._queue_name, durable=True, auto_delete=True)
        for k in self._synapse.event_callback.keys():
            self._channel.queue_bind(queue=self._queue_name, exchange=self._synapse.sys_name,
                                     routing_key="event.%s" % k)

    def run(self):
        self._check_and_create_queue()

        def handler(ch, deliver, props, body):
            if deliver != None:
                if self._synapse.debug:
                    self._synapse.log("Event Receive: %s@%s %s" % (props.type, props.reply_to, str(body)))
                key = "%s.%s" % (props.reply_to, props.type)
                if key in self._synapse.event_callback:
                    res = self._synapse.event_callback[key](json.loads(body), props)
                    if res:
                        self._channel.basic_ack(deliver.delivery_tag)
                    else:
                        self._channel.basic_nack(deliver.delivery_tag)
                else:
                    self._channel.basic_nack(deliver.delivery_tag, requeue=False)

        self._channel.basic_consume(consumer_callback=handler, queue=self._queue_name)
        self._channel.start_consuming()
