package aow

import org.apache.spark.sql
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions.col

/**
 * @param on Determines which column the time window sits on,
 *           the series of data will also be sorted according to this column.
 *           Constrained by Spark window api, this column must be numerical, namely Long or Int,
 *           so use unix timestamp is highly recommended.
 * @author liutianlu
 *         <br/>Created 2022/12/22 15:48
 */
case class TimeWindowMaker(on: sql.Column) {

  private var _window: WindowSpec = _

  def window(partitionColumns: Seq[sql.Column] = Seq(col(DERIVED_DIMS))): TimeWindowMaker = {
    _window = Window
      .partitionBy(partitionColumns: _*)
      .orderBy(on)
    this
  }

  def span(days: Long = 0L,
           hours: Long = 0L,
           minutes: Long = 0L,
           seconds: Long = 0L): TimeWindowMaker = {
    val daySec = days * 24 * 60 * 60
    val hourSec = hours * 60 * 60
    val minSec = minutes * 60
    val totalSec = daySec + hourSec + minSec + seconds
    _window = _window.rangeBetween(-totalSec, Window.currentRow)
    this
  }

  // Normally you must call `window` exact once to build a WindowSpec
  def make(): WindowSpec = {
    _window
  }
}
