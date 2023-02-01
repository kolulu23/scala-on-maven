package aow

import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.expressions.Aggregator

import scala.collection.mutable

/**
 *
 * @author liutianlu
 *         <br/>Created 2023/2/1 21:01
 */
abstract class SetAggregator[T, OUT] extends Aggregator[T, mutable.Set[T], OUT] {
  override def zero: mutable.Set[T] = mutable.LinkedHashSet()

  override def reduce(b: mutable.Set[T], a: T): mutable.Set[T] = {
    b.add(a)
    b
  }

  override def merge(b1: mutable.Set[T], b2: mutable.Set[T]): mutable.Set[T] = b1 ++ b2

  override def bufferEncoder: Encoder[mutable.Set[T]] = Encoders.kryo
}
