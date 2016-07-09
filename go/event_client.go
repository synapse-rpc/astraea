package synapse

import (
	"github.com/streadway/amqp"
	"log"
	"github.com/bitly/go-simplejson"
)

/**
发送一个事件
 */
func (s *Server) SendEvent(action string, params map[string]interface{}) {
	if s.DisableEventClient {
		log.Printf("[Synapse Error] %s: %s \n", "Event Send Not Success", "DisableEventClient set true")
	} else {
		data := map[string]interface{}{
			"action": action,
			"params": params,
		}
		query := simplejson.New();
		query.Set("from", s.AppName)
		query.Set("to", data["action"].(string))
		query.Set("action", "event")
		query.Set("params", data["params"])
		queryJSON, _ := query.MarshalJSON()
		err := s.mqch.Publish(
			s.SysName, // exchange
			"event." + s.AppName + "." + data["action"].(string), // routing key
			false, // mandatory
			false, // immediate
			amqp.Publishing{
				ContentType: "application/json",
				Body:        []byte(queryJSON),
			})
		s.failOnError(err, "Failed to publish a event")
		if s.Debug {
			log.Printf("[Synapse Debug] Publish Event: %s.%s %s", s.AppName, data["action"], queryJSON)
		}

	}
}