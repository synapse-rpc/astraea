package synapse

import (
	"log"
	"github.com/bitly/go-simplejson"
	"github.com/streadway/amqp"
	"time"
)
/**
绑定RPC Callback监听队列
 */
func (s *Server) rpcCallbackQueue() {
	q, err := s.mqch.QueueDeclare(
		s.SysName + "_rpc_cli_" + s.AppName + "_" + s.AppId, // name
		true, // durable
		true, // delete when usused
		false, // exclusive
		false, // no-wait
		nil, // arguments
	)
	s.failOnError(err, "Failed to declare rpcQueue")

	err = s.mqch.QueueBind(
		q.Name,
		"rpc.cli." + s.AppName + "." + s.AppId,
		s.SysName,
		false,
		nil)
	s.failOnError(err, "Failed to Bind Rpc Exchange and Queue")
}

/**
创建 Callback 队列监听
 */
func (s *Server) rpcCallbackQueueListen() {
	s.cli, err = s.mqch.Consume(
		s.SysName + "_rpc_cli_" + s.AppName + "_" + s.AppId, // queue
		"", // consumer
		false, // auto-ack
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil, // args
	)
	s.failOnError(err, "Failed to register Rpc Callback consumer")
	err = s.mqch.Qos(
		1, // prefetch count
		0, // prefetch size
		false, // global
	)
	s.failOnError(err, "Failed to set Rpc Queue QoS")
	log.Printf("[Synapse Info] Rpc Client Handler Listening")
}

/**
RPC Clenit
 */
func (s *Server) rpcClient(data map[string]interface{}, result chan map[string]interface{}) {
	query := simplejson.New();
	query.Set("from", s.AppName + "." + s.AppId)
	query.Set("to", data["appName"].(string))
	query.Set("action", data["action"])
	query.Set("params", data["params"])
	queryJson, _ := query.MarshalJSON()
	corrId := s.randomString(20)
	err = s.mqch.Publish(
		s.SysName, // exchange
		"rpc.srv." + data["appName"].(string), // routing key
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType:   "application/json",
			CorrelationId: corrId,
			ReplyTo:       "rpc.cli." + s.AppName + "." + s.AppId,
			Body:          []byte(queryJson),
		})
	s.failOnError(err, "Failed to publish Rpc Request")
	if s.Debug {
		log.Printf("[Synapse Debug] Publish Rpc Request: %s", queryJson)
	}
	for d := range s.cli {
		if corrId == d.CorrelationId {
			query, _ := simplejson.NewJson(d.Body)
			action := query.Get("action").MustString()
			params := query.Get("params").MustMap()
			if action == "reply-" + data["action"].(string) {
				if s.Debug {
					logData, _ := query.MarshalJSON()
					log.Printf("[Synapse Debug] Receive Rpc Callback: %s", logData)
				}
				d.Ack(false)
				result <- params
			}
			break
		}
	}
}

/**
发起 RPC请求
 */
func (s *Server) SendRpc(appName, action string, params map[string]interface{}) map[string]interface{} {
	if s.DisableEventClient {
		log.Printf("[Synapse Error] %s: %s \n", "Rpc Request Not Send", "DisableRpcClient set true")
		return map[string]interface{}{"Error":"Rpc Request Not Send: DisableRpcClient set true"}
	}
	data := map[string]interface{}{
		"appName": appName,
		"action": action,
		"params": params,
	}
	result := make(chan map[string]interface{})
	go s.rpcClient(data, result)
	select {
	case ret := <-result:
		return ret
	case <-time.After(time.Second * s.RpcTimeout):
		log.Printf("[Synapse Error] %s: %s \n", "Rpc Request Not Success", "Request Timeout")
		return map[string]interface{}{"Error":"Rpc Request Not Success: Request Timeout"}
	}

}