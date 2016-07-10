package synapse

import (
	"github.com/streadway/amqp"
	"log"
	"github.com/bitly/go-simplejson"
)

/**
发送一个事件
 */
func (s *Server) SendEvent(eventName string, params map[string]interface{}) {
	if s.DisableEventClient {
		log.Printf("[Synapse Error] %s: %s \n", "Event Send Not Success", "DisableEventClient set true")
	} else {
		query := simplejson.New();
		query.Set("from", s.AppName)
		query.Set("to", "event")
		query.Set("action", eventName)
		query.Set("params", params)
		queryJSON, _ := query.MarshalJSON()
		err := s.mqch.Publish(
			s.SysName, // exchange
			"event." + s.AppName + "." + eventName, // routing key
			false, // mandatory
			false, // immediate
			amqp.Publishing{
				ContentType: "application/json",
				CorrelationId: s.randomString(20),
				Body:        []byte(queryJSON),
			})
		s.failOnError(err, "Failed to publish a event")
		if s.Debug {
			log.Printf("[Synapse Debug] Publish Event: %s.%s %s", s.AppName, eventName, queryJSON)
		}

	}
}