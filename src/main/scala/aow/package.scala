import org.apache.spark.sql
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import java.time.Instant

/**
 * Aggregators Over Windows
 *
 * @author liutianlu
 *         <br/>Created 2022/12/21 19:14
 */
package object aow {
  /**
   * Column name of concatenated dim fields, to make this column consistent between jobs,
   * user must assure that dim fields are sorted in a way.
   */
  val DERIVED_DIMS = "_union_dims"

  /**
   * Default dim fields separator in [[DERIVED_DIMS]] concatenation
   */
  val DEFAULT_DIM_SEP = "#"

  /**
   * Column name of derived timestamp field. The field can be used in [[Instant.ofEpochMilli]].
   * Data type for this field should be [[LongType]].
   * It is also loosely equivalent to [[org.apache.spark.sql.functions.unix_timestamp]] * 1000,
   * as long as time zone and leap second is not your concerns.
   *
   * Since which column it is derived from is determined at runtime, so this column can only guarantee
   * consistency at input level, (e.g. Same data warehouse and same executor).
   *
   * Your data may not be suitable to derive such a field if it is not event-like or time-series data.
   * In that case, [[DERIVED_DATE]] may be more useful.
   */
  val DERIVED_TIME = "_derived_ts"

  /**
   * For referencing column derived by [[aow.derive_unix_time]].
   */
  val DERIVED_UNIX_TIME = "_derive_uts"

  /**
   * Same as [[DERIVED_TIME]] except it derives to a formatted date string.
   *
   * For some reasons, if you only aggregate data by day, you may only derive
   * this column rather than [[DERIVED_TIME]] or both.
   */
  val DERIVED_DATE = "_derived_dt"

  /**
   * Default format for [[DERIVED_DATE]]
   */
  val DERIVED_DATE_FMT = "yyyy-MM-dd"

  /**
   * Derive a union dim column with the name of [[DERIVED_DIMS]]
   *
   * @param dims Dim fields
   * @param sep  Dim fields separator, default to '#'
   * @return Concatenated dim fields as the derived column
   */
  def derive_dims(dims: Seq[String], sep: String = DEFAULT_DIM_SEP): sql.Column = {
    concat_ws(sep, dims.map(expr): _*)
  }

  /**
   * Derive a time column with the name of [[DERIVED_TIME]] in milliseconds since unix epoch.
   *
   * If `dataType` is [[LongType]], we assume it's already milliseconds since unix epoch.
   *
   * @param src      Column to be derived from
   * @param dataType Data type of `src`
   * @param fmt      Datetime format string of `src`. Set it to `None` if `dataType` is [[LongType]] or
   *                 you don't know the format string of `src`, in later case `yyyy-MM-dd HH:mm:ss` is used.
   * @return Derived time column, data type is long
   */
  def derive_time(src: sql.Column, dataType: DataType, fmt: Option[String]): sql.Column = {
    val dt = dataType match {
      case LongType => src
      case DateType | TimestampType | StringType => fmt.fold(unix_timestamp(src))(unix_timestamp(src, _))
      case _ => src
    }
    dt.cast(LongType)
  }

  /**
   * Derive a time column with the name of [[DERIVED_TIME]] in milliseconds since unix epoch.
   *
   * @param src Column to be derived from
   * @param fmt Datetime format string of `src`. Set it to `None` if `dataType` is [[LongType]] or
   *            you don't know the format string of `src`, in later case `yyyy-MM-dd HH:mm:ss` is used.
   * @see derive_time(src: sql.Column, dataType: DataType, fmt: Option[String]): sql.Column
   * @throws sql.catalyst.analysis.UnresolvedException If `src` is not bond to any dataframe or not being resolved
   * @return Derived time column, data type is long
   */
  def derive_time(src: sql.Column, fmt: Option[String]): sql.Column = derive_time(src, src.expr.dataType, fmt)

  /**
   * Derive a time column with the name of [[DERIVED_TIME]] in seconds since unix epoch.
   *
   * @param src Column to be derived from
   * @param fmt Datetime format string of `src`. Set it to `None` if `dataType` is [[LongType]] or
   *            you don't know the format string of `src`, in later case `yyyy-MM-dd HH:mm:ss` is used.
   * @see derive_time(src: sql.Column, dataType: DataType, fmt: Option[String]): sql.Column
   * @throws sql.catalyst.analysis.UnresolvedException If `src` is not bond to any dataframe or not being resolved
   * @return Derived time column, data type is long
   */
  def derive_unix_time(src: sql.Column, fmt: Option[String]): sql.Column = (derive_time(src, fmt) / 1000L).cast(LongType)

  /**
   * Derive a date column with the name of [[DERIVED_DATE]].
   *
   * If `dataType` is [[LongType]], we assume it's milliseconds passed since unix epoc.
   *
   * @param src      Column name to derive from
   * @param dataType Data type of `from`
   * @return Derived date string column
   */
  def derive_date(src: sql.Column, dataType: DataType): sql.Column = {
    dataType match {
      case LongType => from_unixtime(src / 1000L, DERIVED_DATE_FMT)
      case DateType | TimestampType | StringType => date_format(src, DERIVED_DATE_FMT)
      case _ => src
    }
  }

  /**
   * Derive a date column with the name of DERIVED_DATE using evaluate dataType of `src`.
   *
   * @param src Column name to derive from
   * @return Derived date string column
   * @throws sql.catalyst.analysis.UnresolvedException If `src` is not bond to any dataframe or not being resolved
   */
  def derive_date(src: sql.Column): sql.Column = derive_date(src, src.expr.dataType)
}
