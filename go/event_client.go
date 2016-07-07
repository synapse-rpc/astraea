package synapse

import (
	"github.com/streadway/amqp"
	"log"
	"github.com/bitly/go-simplejson"
)

func eventClient(ch *amqp.Channel, eventSender chan map[string]interface{}) {
	log.Printf(" [*] Event Sender ready")
	for {
		data := <-eventSender
		query := simplejson.New();
		query.Set("from", "icarus")
		query.Set("to", data["action"].(string))
		query.Set("action", "event")
		query.Set("params", data["params"])
		queryJson, _ := query.MarshalJSON()
		err := ch.Publish(
			"event", // exchange
			"icarus." + data["action"].(string), // routing key
			false, // mandatory
			false, // immediate
			amqp.Publishing{
				ContentType: "application/json",
				Body:        []byte(queryJson),
			})
		failOnError(err, "Failed to publish a event")
		log.Printf(" [x] Publish a event: %s %s", data["action"], queryJson)
	}
}