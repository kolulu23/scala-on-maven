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
class SetSerdeAggregator[T] extends SetAggregator[T, Set[T]] {

  override def finish(reduction: mutable.Set[T]): Set[T] = reduction.toSet

  override def outputEncoder: Encoder[Set[T]] = Encoders.kryo
}
