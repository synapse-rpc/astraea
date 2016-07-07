package synapse

import (
	"github.com/streadway/amqp"
	"log"
)

var (
	eventSender chan map[string]interface{}
	rpcSender chan map[string]interface{}
	rpcReceiver chan map[string]interface{}
)
/**
启动 Synapse 组件, 开始监听RPC请求和事件
 */
func Serve(host, port, user, pass string, eventCallbackMap map[string]func(map[string]interface{}, amqp.Delivery),
rpcCallbackMap map[string]func(map[string]interface{}, amqp.Delivery) interface{}) {
	server := "amqp://" + user + ":" + pass + "@" + host + ":" + port
	conn := createConnection(server)
	defer conn.Close()
	ch := createChannel(conn)
	defer ch.Close()
	eventSender = make(chan map[string]interface{})
	rpcSender = make(chan map[string]interface{})
	rpcReceiver = make(chan map[string]interface{})
	go eventServer(ch, eventCallbackMap)
	go eventClient(ch, eventSender)
	go rpcServer(ch, rpcCallbackMap)
	rpcClient(ch, rpcSender, rpcReceiver)
}

/**
发送一个事件
 */
func SendEvent(action string, params map[string]interface{}) {
	query := map[string]interface{}{
		"action": action,
		"params": params,
	}
	eventSender <- query
}

/**
发起 RPC请求
 */
func SendRpc(action string, params map[string]interface{}) map[string]interface{} {
	query := map[string]interface{}{
		"action": action,
		"params": params,
	}
	rpcSender <- query
	return <-rpcReceiver
}

/**
创建到 Rabbit MQ的链接
 */
func createConnection(server string) *amqp.Connection {
	conn, err := amqp.Dial(server)
	failOnError(err, "Failed to connect to RabbitMQ")
	return conn
}

/**
创建到 Rabbit MQ 的通道
 */
func createChannel(conn *amqp.Connection) *amqp.Channel {
	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	return ch
}

/**
便捷报错方法
 */
func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s \n", msg, err)
	}
}
