package com.github.flinkalt.flink.helper

import java.io.IOException
import java.net.{InetAddress, InetSocketAddress, UnknownHostException}

import com.github.flinkalt.Data
import com.github.flinkalt.typeinfo.TypeInfo
import com.github.flinkalt.typeinfo.auto._
import org.apache.flink.runtime.net.ConnectionUtils
import org.apache.flink.streaming.api.environment.{LocalStreamEnvironment, RemoteStreamEnvironment}
import org.apache.flink.streaming.api.scala._

import scala.collection.JavaConverters._

class DataStreamCollector {
  private var toNotify = Vector.empty[SocketStreamIterator[_]]

  def collect[T: TypeInfo](stream: DataStream[T]): StreamCollector[Data[T]] = {
    val env = stream.javaStream.getExecutionEnvironment

    //Find out what IP of us should be given to CollectSink, that it will be able to connect to
    val clientAddress = env match {
      case remoteStreamEnv: RemoteStreamEnvironment =>
        try {
          val host = remoteStreamEnv.getHost
          val port = remoteStreamEnv.getPort
          ConnectionUtils.findConnectingAddress(new InetSocketAddress(host, port), 2000, 400)
        } catch {
          case e: Exception =>
            throw new IOException("Could not determine an suitable network address to receive back data from the streaming program.", e)
        }
      case _: LocalStreamEnvironment =>
        InetAddress.getLoopbackAddress
      case _ => try {
        InetAddress.getLocalHost
      } catch {
        case e: UnknownHostException =>
          throw new IOException("Could not determine this machines own local address to receive back data from the streaming program.", e)
      }
    }

    val dataTypeInfo = implicitly[TypeInfo[Data[T]]]

    val serializer = dataTypeInfo.flinkTypeInfo.createSerializer(stream.executionEnvironment.getConfig)
    val streamIterator = new SocketStreamIterator[Data[T]](serializer)
    val sink = stream.addSink(new CollectSink[T](clientAddress, streamIterator.getPort, serializer))
    sink.setParallelism(1) // It would not work if multiple instances would connect to the same port

    toNotify = toNotify :+ streamIterator
    new StreamCollector(streamIterator.asScala)
  }

  def start(env: StreamExecutionEnvironment): Unit = {
    val thread = new Thread(new Runnable {
      override def run(): Unit = try {
        env.execute
      }
      catch {
        case t: Throwable =>
          toNotify.foreach(_.notifyOfError(t))
      }
    })
    thread.start()
  }
}

class StreamCollector[T](iterator: Iterator[T]) {
  lazy val toVector: Vector[T] = iterator.toVector
}