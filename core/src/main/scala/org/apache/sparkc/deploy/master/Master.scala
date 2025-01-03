import java.util.concurrent.TimeUnit
import akka.actor.{Actor, ActorSystem, Props}
import com.typesafe.config.ConfigFactory
import scala.concurrent.duration.Duration
import org.apache.sparkc.deploy.message._

class Master extends Actor{

  //  override def preStart(): Unit = {
  //    context.system.scheduler.schedule(Duration.Zero, Duration(6000, TimeUnit.MILLISECONDS), self, RemoveTimeOutWorker)
  //  }

  //存储worker信息到HashMap
  val idToWorkerInfoMap = scala.collection.mutable.HashMap[String,WorkerInfo]()

  override def receive = {
    //收到worker注册过来的信息
    case RegisterWorkerInfo(workId, core, ram) => {
      //将worker的信息存储起来,存入HashMap中
      if(!idToWorkerInfoMap.contains(workId)){
        val workerInfo = new WorkerInfo(workId,core,ram)
        idToWorkerInfoMap += ((workId,workerInfo))   //等同于idToWorkerInfoMap.put(workId,workerInfo)
        sender() ! RegisteredWorkerInfo //此时对应的worker会收到注册成功的消息
      }
    }
    case HeartBeat(workId) => {
      if(workId != null && !workId.trim.equals("")){
        //master收到worker的心跳包后更新上一次心跳的时间
        val workerInfo = idToWorkerInfoMap(workId)
        //更新上一次心跳时间
        workerInfo.lastHeartBeatTime = System.currentTimeMillis()
      }
    }
    //接收到自己发来的检查worker超时信息
    case CheckTimeOutWorker => {
      import context.dispatcher //使用调度器的时候必须导入Dispatcher
      //检查策略,周期性(6000ms)的取出两次心跳间隔超过3000ms的worker,并从map中剔除
      context.system.scheduler.schedule(Duration.Zero, Duration(6000, TimeUnit.MILLISECONDS), self, RemoveTimeOutWorker)
    }
    case RemoveTimeOutWorker => {
      //遍历map 查看当前时间和上一次心跳时间差 3000
      val workerInfos = idToWorkerInfoMap.values
      //过滤之后结果是超时的worker,使用foreach删除 没有返回值
      workerInfos.filter(workerInfo => System.currentTimeMillis() - workerInfo.lastHeartBeatTime > 3000 )
        .foreach(workerTimeOutNode => idToWorkerInfoMap.remove(workerTimeOutNode.id))
      println(s"还剩 ${idToWorkerInfoMap.size}个 存活的worker")
    }
  }
}

object SparkMaster{
  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      println("请输入参数 <host> <port> <workName>")
      sys.exit()
    }
    val host = args(0)
    val port = args(1)
    val masterName = args(2)

    val config = ConfigFactory.parseString(
      s"""
         |akka.actor.provider="akka.remote.RemoteActorRefProvider"
         |akka.remote.netty.tcp.hostname=$host
         |akka.remote.netty.tcp.port=$port
      """.stripMargin)
    val sparkWorkerSystem = ActorSystem("sparkMaster", config)
    val workActorRef = sparkWorkerSystem.actorOf(Props[Master], masterName)
    workActorRef ! CheckTimeOutWorker
  }
}