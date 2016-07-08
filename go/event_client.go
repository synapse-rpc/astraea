package synapse

import (
	"github.com/streadway/amqp"
	"log"
	"github.com/bitly/go-simplejson"
)

func eventClient(ch *amqp.Channel, eventSender chan map[string]interface{}) {
	log.Printf("[Synapse Info] Event Sender ready")
	for {
		data := <-eventSender
		query := simplejson.New();
		query.Set("from", appName)
		query.Set("to", data["action"].(string))
		query.Set("action", "event")
		query.Set("params", data["params"])
		queryJson, _ := query.MarshalJSON()
		err := ch.Publish(
			"event", // exchange
			appName + "." + data["action"].(string), // routing key
			false, // mandatory
			false, // immediate
			amqp.Publishing{
				ContentType: "application/json",
				Body:        []byte(queryJson),
			})
		failOnError(err, "Failed to publish a event")
		if debug {
			log.Printf("[Synapse Debug] Publish Event: %s.%s %s", appName, data["action"], queryJson)
		}
	}
}