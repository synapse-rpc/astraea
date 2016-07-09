package synapse

import (
	"github.com/streadway/amqp"
	"log"
	"github.com/bitly/go-simplejson"
)

/**
注册 RPC 的 MQ Exchange
 */
func rpcExchange(ch *amqp.Channel) {
	err := ch.ExchangeDeclare(
		"rpc", // name
		"direct", // type
		true, // durable
		false, // auto-deleted
		false, // internal
		false, // no-wait
		nil, // arguments
	)
	failOnError(err, "Failed to declare Event Exchange")
	return
}

/**
绑定RPC监听队列
 */
func rpcQueue(ch *amqp.Channel) {
	q, err := ch.QueueDeclare(
		"rpc_srv_" + appName, // name
		true, // durable
		true, // delete when usused
		false, // exclusive
		false, // no-wait
		nil, // arguments
	)
	failOnError(err, "Failed to declare rpcQueue")

	err = ch.QueueBind(
		q.Name,
		appName,
		"rpc",
		false,
		nil)
	failOnError(err, "Failed to Bind Rpc Exchange and Queue")

	err = ch.Qos(
		1, // prefetch count
		0, // prefetch size
		false, // global
	)
	failOnError(err, "Failed to set Rpc Queue QoS")
}

/**
创建RPC监听
callback回调为监听到RPC请求后的处理函数
 */
func rpcServer(ch *amqp.Channel, rpcCallbackMap map[string]func(map[string]interface{}, amqp.Delivery) interface{}) {
	rpcExchange(ch)
	rpcQueue(ch)
	msgs, err := ch.Consume(
		"rpc_srv_" + appName, // queue
		"", // consumer
		false, // auto-ack
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil, // args
	)
	failOnError(err, "Failed to register rpcServer consumer")

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			query, _ := simplejson.NewJson(d.Body)
			action := query.Get("action").MustString()
			params := query.Get("params").MustMap()
			if debug {
				logData, _ := query.MarshalJSON()
				log.Printf("[Synapse Debug] Receive Rpc Request: %s", logData)
			}
			var callback func(map[string]interface{}, amqp.Delivery) interface{}
			var ok bool
			var result interface{}
			callback, ok = rpcCallbackMap[action]
			if ok {
				result = callback(params, d)
			} else {
				callback, ok = rpcCallbackMap["*"]
				if ok {
					result = callback(params, d)
				} else {
					result = nil
				}
			}
			response := simplejson.New();
			response.Set("from", appName)
			response.Set("to", d.ReplyTo)
			response.Set("action", "reply-" + action)
			response.Set("params", result)
			resultJson, _ := response.MarshalJSON()
			err = ch.Publish(
				"rpc_cli", // exchange
				d.ReplyTo, // routing key
				false, // mandatory
				false, // immediate
				amqp.Publishing{
					ContentType:   "application/json",
					CorrelationId: d.CorrelationId,
					Body:          []byte(resultJson),
				})
			failOnError(err, "Failed to reply Rpc Request")
			if debug {
				log.Printf("[Synapse Debug] Reply Rpc Request: %s", resultJson)
			}
			d.Ack(false)
		}
	}()

	log.Printf("[Synapse Info] Rpc Handler Listening")
	<-forever
}