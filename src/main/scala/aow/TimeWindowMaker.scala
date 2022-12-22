package aow

import org.apache.spark.sql
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions.col

/**
 *
 * @author liutianlu
 *         <br/>Created 2022/12/22 15:48
 */
case class TimeWindowMaker(on: sql.Column) {

  private var _window: WindowSpec = _

  def window(): TimeWindowMaker = {
    _window = Window
      .partitionBy(col(DERIVED_DIMS))
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

  def make(): WindowSpec = {
    _window
  }
}
