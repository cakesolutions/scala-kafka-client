package cakesolutions.kafka

import org.apache.kafka.clients.consumer.{OffsetAndMetadata, OffsetCommitCallback => JOffsetCommitCallback}
import org.apache.kafka.clients.producer.{Callback, RecordMetadata}
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConverters._
import scala.concurrent.Promise
import scala.util.{Failure, Success, Try}

object OffsetCommitCallback {
  def apply(callback: Try[Offsets] => Unit): JOffsetCommitCallback = new OffsetCommitCallback(callback)
  def apply(promise: Promise[Offsets]): JOffsetCommitCallback = new OffsetCommitCallback(result => promise.complete(result))
}

private[kafka] class OffsetCommitCallback(callback: Try[Offsets] => Unit) extends JOffsetCommitCallback {
  override def onComplete(offsets: java.util.Map[TopicPartition, OffsetAndMetadata], exception: Exception): Unit = {
    val result =
      if (exception == null) Success(Offsets(offsets.asScala.toMap.mapValues(_.offset())))
      else Failure(exception)
    callback(result)
  }
}

private[kafka] object WriteCallback {
  def apply(callback: Try[RecordMetadata] => Unit): Callback = new WriteCallback(callback)
  def apply(promise: Promise[RecordMetadata]): Callback = new WriteCallback(result => promise.complete(result))
}

private[kafka] final class WriteCallback(callback: Try[RecordMetadata] => Unit) extends Callback {
  override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
    val result =
      if (exception == null) Success(metadata)
      else Failure(exception)
    callback(result)
  }
}