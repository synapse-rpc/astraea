package synapse

import (
	"github.com/streadway/amqp"
	"log"
	"github.com/bitly/go-simplejson"
)

/**
绑定事件监听队列
 */
func (s *Server) eventQueue() {
	q, err := s.mqch.QueueDeclare(
		s.SysName + "_event_" + s.AppName, // name
		true, // durable
		false, // delete when usused
		false, // exclusive
		false, // no-wait
		nil, // arguments
	)
	s.failOnError(err, "Failed to declare event queue")

	for k, _ := range s.EventCallbackMap {
		err = s.mqch.QueueBind(
			q.Name, // queue name
			"event." + k, // routing key
			s.SysName, // exchange
			false,
			nil)
		s.failOnError(err, "Failed to bind event queue: " + k)
	}
}

/**
创建事件监听
callback回调为监听到事件后的处理函数
 */
func (s *Server) eventServer() {
	s.eventQueue()
	msgs, err := s.mqch.Consume(
		s.SysName + "_event_" + s.AppName, // queue
		"", // consumer
		false, // auto-ack
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil, // args
	)
	s.failOnError(err, "Failed to register event consumer")
	log.Printf("[Synapse Info] Event Server Handler Listening")
	for d := range msgs {
		query, _ := simplejson.NewJson(d.Body)
		action := query.Get("action").MustString()
		params := query.Get("params").MustMap()
		if action == "event" {
			if s.Debug {
				logData, _ := query.MarshalJSON()
				log.Printf("[Synapse Debug] Receive Event: %s %s", d.RoutingKey, logData)
			}
			var callback func(map[string]interface{}, amqp.Delivery)
			var ok bool
			callback, ok = s.EventCallbackMap[d.RoutingKey]
			if ok {
				callback(params, d)
			} else {
				callback, ok = s.EventCallbackMap["*"]
				if ok {
					callback(params, d)
				}
			}
			d.Ack(false)
		}
	}
}
