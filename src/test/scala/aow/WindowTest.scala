package aow

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, count, from_unixtime, unix_timestamp}
import prelude.FakeSomeData.TRANS_DATA_PATH
import prelude.{FakeSomeData, SparkFunSuite}

/**
 *
 * @author liutianlu
 *         <br/>Created 2022/12/22 15:51
 */
class WindowTest extends SparkFunSuite {

  var df: DataFrame = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    df = sparkSession.read.option("header", value = true).schema(FakeSomeData.TRANS_DATA_SCHEMA).csv(TRANS_DATA_PATH)
    df = df.withColumn(DERIVED_DIMS, derive_dims(Seq("dim", "sub_dim")))
      .withColumn(DERIVED_DATE, derive_date(df("biz_time")))
      .withColumn(DERIVED_UNIX_TIME, derive_unix_time(df("biz_time"), None))
  }

  test("Test TimeWindow") {
    val last3days = TimeWindowMaker(unix_timestamp(col(DERIVED_DATE), DERIVED_DATE_FMT))
      .window()
      .span(days = 3)
      .make()
    val last12minuets = TimeWindowMaker(col(DERIVED_UNIX_TIME))
      .window()
      .span(minutes = 12)
      .make()
    // Note here we use DERIVED_TIME which makes a more accurate sliding window
    val last2dayAnd12hours = TimeWindowMaker(col(DERIVED_UNIX_TIME))
      .window()
      .span(days = 2, hours = 12)
      .make()
    df.select(
      col(DERIVED_DIMS),
      col(DERIVED_DATE),
      from_unixtime(col(DERIVED_UNIX_TIME)).as(DERIVED_UNIX_TIME),
      (count(DERIVED_DIMS) over last3days).as("cnt3d"),
      (count(DERIVED_DIMS) over last12minuets).as("cnt12m"),
      (count(DERIVED_DIMS) over last2dayAnd12hours).as("cnt2d12h")
    ).show(100)
  }
}
