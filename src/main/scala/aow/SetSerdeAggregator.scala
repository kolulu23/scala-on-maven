package aow

import org.apache.spark.sql.{Encoder, Encoders}

import scala.collection.mutable

/**
 * Set aggregator with a upper limit of set size
 *
 * Internally the Set is insertion ordered, but don't rely on that very much cause
 * aggregator may not run sequentially.
 *
 * The output of this aggregator is serialized with kryo and stored as sparks binary type.
 *
 * @author liutianlu
 *         <br/>Created 2022/12/29 14:21
 */
class SetSerdeAggregator[T](override val limit: Long) extends SetAggregator[T, Set[T]](limit) {

  override def finish(reduction: mutable.Set[T]): Set[T] = reduction.toSet

  override def outputEncoder: Encoder[Set[T]] = Encoders.kryo
}

object SetSerdeAggregator {
  def apply[T](): SetSerdeAggregator[T] = new SetSerdeAggregator(limit = SetAggregator.DEFAULT_BUF_LIMIT)

  def apply[T](limit: Long): SetSerdeAggregator[T] = new SetSerdeAggregator(limit)
}
