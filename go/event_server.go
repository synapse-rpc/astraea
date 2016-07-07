package synapse

import (
	"github.com/streadway/amqp"
	"log"
	"github.com/bitly/go-simplejson"
)

/**
注册 事件 的 MQ Exchange
 */
func eventExchage(ch *amqp.Channel) {
	err := ch.ExchangeDeclare(
		"event", // name
		"topic", // type
		true, // durable
		false, // auto-deleted
		false, // internal
		false, // no-wait
		nil, // arguments
	)
	failOnError(err, "Failed to declare Rpc exchange")
}

/**
绑定事件监听队列
 */
func eventQueue(ch *amqp.Channel, eventCallbackMap map[string]func(map[string]interface{}, amqp.Delivery)) {
	q, err := ch.QueueDeclare(
		"event_icarus", // name
		true, // durable
		false, // delete when usused
		false, // exclusive
		false, // no-wait
		nil, // arguments
	)
	failOnError(err, "Failed to declare event queue")

	for k, _ := range eventCallbackMap {
		err = ch.QueueBind(
			q.Name, // queue name
			k, // routing key
			"event", // exchange
			false,
			nil)
		failOnError(err, "Failed to bind event queue: " + k)
	}
}

/**
创建事件监听
callback回调为监听到事件后的处理函数
 */
func eventServer(ch *amqp.Channel, eventCallbackMap map[string]func(map[string]interface{}, amqp.Delivery)) {
	eventExchage(ch)
	eventQueue(ch, eventCallbackMap)
	msgs, err := ch.Consume(
		"event_icarus", // queue
		"", // consumer
		false, // auto-ack
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil, // args
	)
	failOnError(err, "Failed to register event consumer")

	forever := make(chan bool)
	go func() {
		for d := range msgs {
			query, _ := simplejson.NewJson(d.Body)
			action := query.Get("action").MustString()
			params := query.Get("params").MustMap()
			if action == "event" {
				var callback func(map[string]interface{}, amqp.Delivery)
				var ok bool
				callback, ok = eventCallbackMap[d.RoutingKey]
				if ok {
					callback(params, d)
				} else {
					callback, ok = eventCallbackMap["*"]
					if ok {
						callback(params, d)
					}
				}
				d.Ack(false)
			}
		}
	}()

	log.Printf(" [*] Event Handler Listening")
	<-forever
}
