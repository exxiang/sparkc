package org.apache.sparkc.deploy.message

//worker -> master
case class RegisterWorkerInfo(id: String, core: Int, ram: Int)

//worker给Master发送心跳信息(需要告知Master是谁)
case class HeartBeat(id: String)

//master -> worker
//master向worker发送注册成功的消息
case object RegisteredWorkerInfo

//Worker自己发送给自己  意味着需要定期向Master发送心跳信息
case object SendHeartBeat

//master给自己发送一个检查worker超时的信息,并启动一个调度器,周期性检查超时的worker
case object CheckTimeOutWorker

//master发送给自己的消息 删除超时的worker
case object RemoveTimeOutWorker

//存储worker信息的对象类
class WorkerInfo(val id: String, core: Int, ram: Int) {
  var lastHeartBeatTime: Long = _ //初始值为0
}
