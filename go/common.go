package synapse

import (
	"github.com/streadway/amqp"
	"log"
	"time"
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
	RpcTimeout         time.Duration

	conn               *amqp.Connection
	mqch               *amqp.Channel
	cli                <-chan amqp.Delivery
}

var err error

/**
创建一个新的Synapse
 */
func New() *Server {
	return &Server{}
}

/**
启动 Synapse 组件, 开始监听RPC请求和事件
 */
func (s *Server) Serve() {
	if s.AppName == "" || s.SysName == "" {
		log.Fatalf("[Synapse Error] Must Set SysName and AppName system exit .")
	} else {
		log.Print("[Synapse Info] System App Name: " + s.SysName)
		log.Print("[Synapse Info] System App Name: " + s.AppName)
	}
	forever := make(chan bool)
	if s.Debug {
		log.Print("[Synapse Warn] System Run Mode: Debug")
	} else {
		log.Print("[Synapse Info] System Run Mode: Production")
	}
	s.createConnection()
	defer s.conn.Close()
	s.createChannel()
	defer s.mqch.Close()
	s.checkAndCreateExchange()
	if s.EventCallbackMap != nil {
		go s.eventServer()
	} else {
		log.Printf("[Synapse Warn] Event Handler Disabled: EventCallbackMap not set")
	}
	if s.RpcCallbackMap != nil {
		go s.rpcServer()
	} else {
		log.Printf("[Synapse Warn] Rpc Handler Disabled: RpcCallbackMap not set")
	}
	if s.DisableEventClient {
		log.Printf("[Synapse Warn] Event Sender Disabled: DisableEventClient set true")
	}
	if !s.DisableRpcClient {
		s.rpcCallbackQueue()
		go s.rpcCallbackQueueListen()
		if s.RpcTimeout == 0 {
			s.RpcTimeout = 2
		}
	} else {
		log.Printf("[Synapse Warn] Rpc Sender Disabled: DisableRpcClient set true")
	}
	<-forever
}

/**
创建到 Rabbit MQ的链接
 */
func (s *Server) createConnection() {
	s.conn, err = amqp.Dial("amqp://" + s.MqUser + ":" + s.MqPass + "@" + s.MqHost + ":" + s.MqPort)
	s.failOnError(err, "Failed to connect to RabbitMQ")
	log.Print("[Synapse Info] Rabbit MQ Connection Created.")
}

/**
创建到 Rabbit MQ 的通道
 */
func (s *Server) createChannel() {
	s.mqch, err = s.conn.Channel()
	s.failOnError(err, "Failed to open a channel")
	log.Print("[Synapse Info] Rabbit MQ Channel Created.")
}

/**
注册通讯用的 MQ Exchange
 */
func (s *Server) checkAndCreateExchange() {
	err := s.mqch.ExchangeDeclare(
		s.SysName, // name
		"topic", // type
		true, // durable
		false, // auto-deleted
		false, // internal
		false, // no-wait
		nil, // arguments
	)
	s.failOnError(err, "Failed to declare Event Exchange")
	return
}

/**
便捷报错方法
 */
func (s *Server) failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("[Synapse Error] %s: %s \n", msg, err)
	}
}
