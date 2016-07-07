package synapse

import (
	"github.com/streadway/amqp"
	"log"
	"github.com/bitly/go-simplejson"
	"math/rand"
)

/**
注册 RPC Callback的 MQ Exchange
 */
func rpcCallbackExchange(ch *amqp.Channel) {
	err := ch.ExchangeDeclare(
		"rpc_cli", // name
		"fanout", // type
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
绑定RPC Callback监听队列
 */
func rpcCallbackQueue(ch *amqp.Channel) {
	q, err := ch.QueueDeclare(
		"rpc_cli_icarus", // name
		true, // durable
		false, // delete when usused
		false, // exclusive
		false, // no-wait
		nil, // arguments
	)
	failOnError(err, "Failed to declare rpcQueue")

	err = ch.QueueBind(
		q.Name,
		"icarus",
		"rpc_cli",
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

func rpcClient(ch *amqp.Channel, rpcSender chan map[string]interface{}, rpcReceiver chan map[string]interface{}) {
	rpcCallbackExchange(ch)
	rpcCallbackQueue(ch)
	msgs, err := ch.Consume(
		"rpc_cli_icarus", // queue
		"", // consumer
		false, // auto-ack
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil, // args
	)
	failOnError(err, "Failed to register Rpc Callback consumer")
	log.Printf(" [*] Rpc Sender ready")
	for {
		data := <-rpcSender
		query := simplejson.New();
		query.Set("from", "icarus")
		query.Set("to", data["action"].(string))
		query.Set("action", data["params"].(map[string]interface{})["action"].(string))
		query.Set("params", data["params"])
		//query.Get("params").Del("action")
		queryJson, _ := query.MarshalJSON()
		corrId := randomString(20)
		err = ch.Publish(
			"rpc", // exchange
			data["action"].(string), // routing key
			false, // mandatory
			false, // immediate
			amqp.Publishing{
				ContentType:   "application/json",
				CorrelationId: corrId,
				ReplyTo:       "icarus",
				Body:          []byte(queryJson),
			})
		failOnError(err, "Failed to publish Rpc Request")
		for d := range msgs {
			if corrId == d.CorrelationId {
				query, _ := simplejson.NewJson(d.Body)
				action := query.Get("action").MustString()
				params := query.Get("params").MustMap()
				if action == "reply-" + data["params"].(map[string]interface{})["action"].(string) {
					d.Ack(false)
					rpcReceiver <- params
				}
				break
			}
		}
	}
}

func randomString(l int) string {
	bytes := make([]byte, l)
	for i := 0; i < l; i++ {
		bytes[i] = byte(randInt(65, 90))
	}
	return string(bytes)
}
func randInt(min int, max int) int {
	return min + rand.Intn(max - min)
}