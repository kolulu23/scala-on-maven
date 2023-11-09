package aow

import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.expressions.Aggregator

import scala.collection.mutable

/**
 *
 * @param limit The maximum size of this aggregator can temporarily hold. Setting this value prevents
 *              internal state gets too large in an aggregation. For example, use this aggregator on
 *              a large window on high cardinality columns may cause OOM if `limit` is not handled.
 * @author liutianlu
 *         <br/>Created 2023/2/1 21:01
 */
abstract class SetAggregator[T, OUT](val limit: Long = SetAggregator.DEFAULT_BUF_LIMIT)
  extends Aggregator[T, mutable.Set[T], OUT] {

  override def zero: mutable.Set[T] = mutable.LinkedHashSet()

  override def reduce(b: mutable.Set[T], a: T): mutable.Set[T] = {
    if (b.size < this.limit) {
      b.add(a)
    }
    b
  }

  override def merge(b1: mutable.Set[T], b2: mutable.Set[T]): mutable.Set[T] = {
    val buf: mutable.Set[T] = mutable.Set()
    for ((elemA, elemB) <- b1.zipAll(b2, null, null)) {
      if (buf.size >= this.limit) return buf
      if (elemA != null) buf.add(elemA.asInstanceOf[T])
      if (elemB != null) buf.add(elemB.asInstanceOf[T])
    }
    buf
  }

  override def bufferEncoder: Encoder[mutable.Set[T]] = Encoders.kryo
}

object SetAggregator {
  val DEFAULT_BUF_LIMIT: Long = 300L
}
