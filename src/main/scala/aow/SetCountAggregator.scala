package aow

import org.apache.spark.sql.{Encoder, Encoders}

import scala.collection.mutable

/**
 *
 * @author liutianlu
 *         <br/>Created 2023/2/1 19:34
 */
class SetCountAggregator[T] extends SetAggregator[T, Long] {

  override def finish(reduction: mutable.Set[T]): Long = reduction.size.toLong

  override def outputEncoder: Encoder[Long] = Encoders.scalaLong
}
