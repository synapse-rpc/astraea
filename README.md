## 西纳普斯 - synapse (Python Version)

### 此为系统核心交互组件,包含了事件和RPC系统
包地址
> https://pypi.python.org/pypi/astraea

git:
> git clone https://github.com/synapse-rpc/astraea.git synapse

或者使用PIP安装：
> pip install astraea

初始化方法:

```python
#使用pip安装
from synapse import Synapse
#创建一个新的对象(这里有疑问,是不是应该加括号)
server = Synapse()
#定义事件回调
server.event_callback_map = {
    "icarus.test": callback,
    "pytest.test": callback
}
#定义RPC服务方法
server.rpc_callback_map = {
    "pyt.get": pyt,
}
#设置系统名称(相同的系统中的APP才能相互调用)
server.sys_name = ""
#设置应用名称(RPC调用和事件的标识)
server.app_name = ""
#RabbitMQ 服务器地址
server.mq_host = ""
#RabbitMQ 服务器端口
server.mq_port = 5672
#RabbitMQ 服务器用户
server.mq_user = ""
#RabbitMQ 服务器密码
server.mq_pass =""
#调试模式开关 (打开后可以看到很多LOG)
server.debug = True
#是否禁用RPC客户端功能 (默认可以进行RPC请求)
server.disable_rpc_client = True
#是否禁用发送事件的机能 (默认允许发送事件)
server.disable_event_client = True
#开始服务
server.serve()
```
事件处理方法类型:
```python
callback(params, raw) 
#params 为字典,客户端请求数据
#raw 为RPC传输的数据包,一般情况不使用
#需要返回 True表示处理完成,返回False表示处理失败
```
RPC服务方法类型:
```python
pyt(params, raw) 
#params 为字典,客户端请求数据
#raw 为RPC传输的数据包,一般情况不使用
#需要返回 一个key为string的字典
```
发送RPC请求:
```python
#第一个参数为要调用组件的名称
#第二个参数为要调用组件的方法
#第三个参数为一个key为string的字典 要发送的数据
server.send_rpc("icarus","echo",{"ceshi":"我是中文","test":"from python"})
```
发送事件请求:
```python
#第一个参数为要触发的事件名称 
#第二个参数为 事件的相关数据 一个key为string的字典
server.send_event("test",{"ceshi":"我是中文","test":"from python"})
```
上面发送了一个名为 app_name.test 的事件, 只需要在监听器中注册 app_name.test 即可在产生事件时被通知