package com.github.flinkalt.flink.helper

import java.io.{IOException, OutputStream}
import java.net.{InetAddress, Socket}

import com.github.flinkalt.Data
import com.github.flinkalt.time.Instant
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.memory.DataOutputViewStreamWrapper
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

/**
  * Creates a CollectSink that will send the data to the specified host.
  *
  * @param hostIp     IP address of the Socket server.
  * @param port       Port of the Socket server.
  * @param serializer A serializer for the data.
  */
class CollectSink[IN](val hostIp: InetAddress, val port: Int, val serializer: TypeSerializer[Data[IN]]) extends RichSinkFunction[IN] {

  private var client: Socket = _
  private var outputStream: OutputStream = _
  private var streamWriter: DataOutputViewStreamWrapper = _

  /**
    * Initialize the connection with the Socket in the server.
    *
    * @param parameters Configuration.
    */
  override def open(parameters: Configuration): Unit = {
    try {
      client = new Socket(hostIp, port)
      outputStream = client.getOutputStream
      streamWriter = new DataOutputViewStreamWrapper(outputStream)
    } catch {
      case e: IOException =>
        throw new IOException("Cannot connect to the client to send back the stream", e)
    }
  }

  override def invoke(value: IN, context: SinkFunction.Context[_]): Unit = {
    try {
      val time = Instant(context.timestamp())
      val watermark = Instant(context.currentWatermark())
      val data = Data(time, watermark, value)
      serializer.serialize(data, streamWriter)
    }
    catch {
      case e: Exception =>
        throw new IOException("Error sending data back to client (" + hostIp.toString + ":" + port + ')', e)
    }
  }

  /**
    * Closes the connection with the Socket server.
    */
  override def close(): Unit = {
    try {
      if (outputStream != null) {
        outputStream.flush()
        outputStream.close()
      }
      // first regular attempt to cleanly close. Failing that will escalate
      if (client != null) client.close()
    } catch {
      case e: Exception =>
        throw new IOException("Error while closing connection that streams data back to client at " + hostIp.toString + ":" + port, e)
    } finally {
      // if we failed prior to closing the client, close it
      if (client != null) {
        try {
          client.close()
        }
        catch {
          case _: Throwable =>
          // best effort to close, we do not care about an exception here any more
        }
      }
    }
  }
}