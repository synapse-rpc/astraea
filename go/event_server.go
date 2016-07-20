package synapse

import (
	"log"
	"github.com/bitly/go-simplejson"
	"strings"
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
		params := query.Get("params").MustMap()
		if query.Get("to").MustString() == "event" {
			if s.Debug {
				logData, _ := query.MarshalJSON()
				log.Printf("[Synapse Debug] Receive Event: %s.%s %s", query.Get("from").MustString(), query.Get("action").MustString(), logData)
			}
			log.Print(strings.Split(query.Get("from").MustString(), ".")[0] + "." + query.Get("action").MustString())
			callback, ok := s.EventCallbackMap[strings.Split(query.Get("from").MustString(), ".")[0] + "." + query.Get("action").MustString()]
			if (ok && callback(params, d)) {
				d.Ack(false)
			} else {
				d.Reject(true)
			}
		}
	}
}
