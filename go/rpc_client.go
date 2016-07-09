package synapse

import (
	"github.com/streadway/amqp"
	"log"
	"github.com/bitly/go-simplejson"
	"math/rand"
)
/**
绑定RPC Callback监听队列
 */
func rpcCallbackQueue(ch *amqp.Channel) {
	q, err := ch.QueueDeclare(
		sysName + "_rpc_cli_" + appName, // name
		true, // durable
		true, // delete when usused
		false, // exclusive
		false, // no-wait
		nil, // arguments
	)
	failOnError(err, "Failed to declare rpcQueue")

	err = ch.QueueBind(
		q.Name,
		"rpc.cli." + appName,
		sysName,
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
	rpcCallbackQueue(ch)
	msgs, err := ch.Consume(
		sysName + "_rpc_cli_" + appName, // queue
		"", // consumer
		false, // auto-ack
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil, // args
	)
	failOnError(err, "Failed to register Rpc Callback consumer")
	log.Printf("[Synapse Info] Rpc Sender ready")
	for {
		data := <-rpcSender
		query := simplejson.New();
		query.Set("from", appName)
		query.Set("to", data["action"].(string))
		query.Set("action", data["params"].(map[string]interface{})["action"].(string))
		query.Set("params", data["params"])
		queryJson, _ := query.MarshalJSON()
		corrId := randomString(20)
		err = ch.Publish(
			sysName, // exchange
			"rpc.srv." + data["action"].(string), // routing key
			false, // mandatory
			false, // immediate
			amqp.Publishing{
				ContentType:   "application/json",
				CorrelationId: corrId,
				ReplyTo:       "rpc.cli." + appName,
				Body:          []byte(queryJson),
			})
		failOnError(err, "Failed to publish Rpc Request")
		if debug {
			log.Printf("[Synapse Debug] Publish Rpc Request: %s", queryJson)
		}
		for d := range msgs {
			if corrId == d.CorrelationId {
				query, _ := simplejson.NewJson(d.Body)
				action := query.Get("action").MustString()
				params := query.Get("params").MustMap()
				if action == "reply-" + data["params"].(map[string]interface{})["action"].(string) {
					if debug {
						logData, _ := query.MarshalJSON()
						log.Printf("[Synapse Debug] Receive Rpc Callback: %s", logData)
					}
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