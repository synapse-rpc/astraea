package synapse

import (
	"github.com/streadway/amqp"
	"log"
)

var (
	eventSender chan map[string]interface{}
	rpcSender chan map[string]interface{}
	rpcReceiver chan map[string]interface{}
	appName string
	sysName string
	debug bool
	disableRpcClient bool
	disableEventClient bool
)

type Server struct {
	Debug              bool
	DisableRpcClient   bool
	DisableEventClient bool
	AppName            string
	SysName            string
	MqHost             string
	MqPort             string
	MqUser             string
	MqPass             string
	EventCallbackMap   map[string]func(map[string]interface{}, amqp.Delivery)
	RpcCallbackMap     map[string]func(map[string]interface{}, amqp.Delivery) interface{}
}

/**
启动 Synapse 组件, 开始监听RPC请求和事件
 */
func (s Server) Serve() {
	debug = s.Debug
	disableRpcClient = s.DisableRpcClient
	disableEventClient = s.DisableEventClient
	if s.AppName == "" || s.SysName == "" {
		log.Fatalf("[Synapse Error] Must Set SysName and AppName system exit .")
	} else {
		sysName = s.SysName
		appName = s.AppName
		log.Print("[Synapse Info] System App Name: " + appName)
	}
	forever := make(chan bool)
	if s.Debug {
		log.Print("[Synapse Warn] System Run Mode: Debug")
	} else {
		log.Print("[Synapse Info] System Run Mode: Production")
	}
	server := "amqp://" + s.MqUser + ":" + s.MqPass + "@" + s.MqHost + ":" + s.MqPort
	conn := createConnection(server)
	defer conn.Close()
	ch := createChannel(conn)
	defer ch.Close()
	checkAndCreateExchange(ch)
	eventSender = make(chan map[string]interface{})
	rpcSender = make(chan map[string]interface{})
	rpcReceiver = make(chan map[string]interface{})
	if s.EventCallbackMap != nil {
		go eventServer(ch, s.EventCallbackMap)
	} else {
		log.Printf("[Synapse Warn] Event Handler Disabled: EventCallbackMap not set")
	}
	if s.RpcCallbackMap != nil {
		go rpcServer(ch, s.RpcCallbackMap)
	} else {
		log.Printf("[Synapse Warn] Rpc Handler Disabled: RpcCallbackMap not set")
	}
	if !s.DisableEventClient {
		go eventClient(ch, eventSender)
	} else {
		log.Printf("[Synapse Warn] Event Sender Disabled: DisableEventClient set true")
	}
	if !s.DisableRpcClient {
		go rpcClient(ch, rpcSender, rpcReceiver)
	} else {
		log.Printf("[Synapse Warn] Rpc Sender Disabled: DisableRpcClient set true")
	}
	<-forever
}

/**
发送一个事件
 */
func SendEvent(action string, params map[string]interface{}) {
	if disableEventClient {
		log.Printf("[Synapse Error] %s: %s \n", "Event Send Not Success", "DisableEventClient set true")
	} else {
		query := map[string]interface{}{
			"action": action,
			"params": params,
		}
		eventSender <- query

	}
}

/**
发起 RPC请求
 */
func SendRpc(action string, params map[string]interface{}) map[string]interface{} {
	if disableEventClient {
		log.Printf("[Synapse Error] %s: %s \n", "Rpq Request Not Send", "DisableRpcClient set true")
		return nil
	}
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
注册通讯用的 MQ Exchange
 */
func checkAndCreateExchange(ch *amqp.Channel) {
	err := ch.ExchangeDeclare(
		sysName, // name
		"topic", // type
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
便捷报错方法
 */
func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("[Synapse Error] %s: %s \n", msg, err)
	}
}
