import java.util.UUID
import java.util.concurrent.TimeUnit
import akka.actor.{Actor, ActorSelection, ActorSystem, Props}
import com.typesafe.config.ConfigFactory
import scala.concurrent.duration._ //导入时间单位
import org.apache.sparkc.deploy.message._

//Worker向Master注册自己的信息

class Worker(masterURL: String) extends Actor {
  //master的actorRef
  var masterProxy: ActorSelection = _
  val workId = UUID.randomUUID().toString

  override def preStart(): Unit = {
    masterProxy = context.actorSelection(masterURL)
  }

  override def receive = {
    case "started" => { //自己已就绪
      //向master注册自己的信息(id,核数,内存大小)
      masterProxy ! RegisterWorkerInfo(workId, 4, 32 * 1024)
    }
    case RegisteredWorkerInfo => { //Master给Worker发送的成功信息
      import context.dispatcher //使用调度器的时候必须导入Dispatcher
      //接收到Master发来的成功消息后,worker启动一个定时器,定时地向Master发送心跳信息
      context.system.scheduler.schedule(Duration.Zero, Duration(1500, TimeUnit.MILLISECONDS), self, SendHeartBeat)
    }
    case SendHeartBeat => {
      //向Master发送心跳
      masterProxy ! HeartBeat(workId) //此时Master将会收到心跳消息
      println(s"--------------- $workId 发送心跳 ---------------")
    }
  }

}

object SparkWorker {

  def main(args: Array[String]): Unit = {
    if (args.length != 4) {
      println("请输入参数 <host> <port> <workName> <masterURL>")
      sys.exit()
    }
    val host = args(0)
    val port = args(1)
    val workerName = args(2)
    val masterURL = args(3)

    val config = ConfigFactory.parseString(
      s"""
         |akka.actor.provider="akka.remote.RemoteActorRefProvider"
         |akka.remote.netty.tcp.hostname=$host
         |akka.remote.netty.tcp.port=$port
      """.stripMargin)
    val sparkWorkerSystem = ActorSystem("sparkWorker", config)
    val workActorRef = sparkWorkerSystem.actorOf(Props(new Worker(masterURL)), workerName)
    workActorRef ! "started"
  }
}