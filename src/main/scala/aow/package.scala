import org.apache.spark.sql
import org.apache.spark.sql.functions.{col, concat_ws, date_format, expr, from_unixtime, unix_timestamp}
import org.apache.spark.sql.types.{DataType, DateType, LongType, StringType, TimestampType}

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
   */
  val DERIVED_TIME = "_derived_ts"

  /**
   * Same as [[DERIVED_TIME]] except it derives to a formatted date string.
   */
  val DERIVED_DATE = "_derived_dt"

  /**
   * Default format for [[DERIVED_DATE]]
   */
  val DEFAULT_DATE_FMT = "yyyy-MM-dd"

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
   * Derive a time column with the name of [[DERIVED_TIME]].
   *
   * If `dataType` is [[LongType]], we assume it's milliseconds passed since unix epoc.
   *
   * @param src      Column to be derived from
   * @param dataType Data type of `from`
   * @param fmt      Format string of `from`, leave it to None if `dataType` is [[LongType]]
   * @return Derived time column
   */
  def derive_time(src: sql.Column,
                  dataType: DataType,
                  fmt: Option[String]): sql.Column = {
    dataType match {
      case LongType => src.cast(LongType)
      case DateType | TimestampType | StringType => fmt.fold(unix_timestamp(src))(unix_timestamp(src, _)).cast(LongType)
      case _ => src
    }
  }

  /**
   * Derive a date column with the name of [[DERIVED_DATE]].
   *
   * If `dataType` is [[LongType]], we assume it's milliseconds passed since unix epoc.
   *
   * @param src      Column name to derive from
   * @param dataType Data type of `from`
   * @param fmt      Format string for the derived column
   * @return Derived date string column
   */
  def derive_date(src: sql.Column,
                  dataType: DataType,
                  fmt: String = DEFAULT_DATE_FMT): sql.Column = {
    dataType match {
      case LongType => from_unixtime(src / 1000L, fmt)
      case DateType | TimestampType | StringType => date_format(src, fmt)
      case _ => src
    }
  }
}
