package aow

import org.apache.spark.sql.{Encoder, Encoders}

import scala.collection.mutable

/**
 *
 * @author liutianlu
 *         <br/>Created 2023/2/1 19:34
 */
class SetCountAggregator[T](override val limit: Long) extends SetAggregator[T, Long](limit) {

  override def finish(reduction: mutable.Set[T]): Long = reduction.size.toLong

  override def outputEncoder: Encoder[Long] = Encoders.scalaLong
}

object SetCountAggregator {
  def apply[T](): SetCountAggregator[T] = new SetCountAggregator(limit = SetAggregator.DEFAULT_BUF_LIMIT)

  def apply[T](limit: Long): SetCountAggregator[T] = new SetCountAggregator(limit)
}
