package aow

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, functions}
import prelude.FakeSomeData._
import prelude.SparkFunSuite

/**
 *
 * @author liutianlu
 *         <br/>Created 2022/12/29 14:52
 */
class AggregatorTest extends SparkFunSuite {
  var df: DataFrame = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    df = sparkSession.read.option("header", value = true)
      .schema(TRANS_DATA_SCHEMA)
      .csv(TRANS_DATA_PATH_SMALL)
    df = df.withColumn(DERIVED_DIMS, derive_dims(Seq(TRANS_DATA_FIELD_DIM.name, TRANS_DATA_FIELD_SUB_DIM.name)))
      .withColumn(DERIVED_DATE, derive_date(df(TRANS_DATA_FIELD_BIZ_TIME.name)))
      .withColumn(DERIVED_UNIX_TIME, derive_unix_time(df(TRANS_DATA_FIELD_BIZ_TIME.name), None))
  }

  test("Predefined Aggregator, Days, One Dim") {
    val tw = TimeWindowMaker(unix_timestamp(col(DERIVED_DATE), DERIVED_DATE_FMT)).window()
    val window1Day = tw.span(days = 1).make()
    val window3Day = tw.span(days = 3).make()
    val cnt1d = (count(DERIVED_DIMS) over window1Day).as("cnt1d")
    val cnt3d = (count(DERIVED_DIMS) over window3Day).as("cnt3d")
    val sum3d = (sum(TRANS_DATA_FIELD_TRANS_AMOUNT.name) over window3Day).as("sum3d")
    val fields = Seq(
      col(DERIVED_DIMS),
      col(DERIVED_DATE),
      col(DERIVED_UNIX_TIME),
      cnt1d,
      cnt3d,
      sum3d
    )
    df.select(fields: _*)
      .dropDuplicates(DERIVED_DIMS, DERIVED_DATE)
      .show(100)
  }

  test("SetAgg") {
    val window = TimeWindowMaker(unix_timestamp(col(DERIVED_DATE), DERIVED_DATE_FMT))
      .window()
      .span(days = 3)
      .make()
    sparkSession.udf.register("setCountAgg", functions.udaf(new SetCountAggregator[String]))
    sparkSession.udf.register("setSerdeAgg", functions.udaf(new SetSerdeAggregator[String]))
    val setCount3d = (expr(s"setCountAgg(${TRANS_DATA_FIELD_TARGET_ID.name})") over window).as("set3d")
    val setSerde3d = (expr(s"setSerdeAgg(${TRANS_DATA_FIELD_TARGET_ID.name})") over window).as("set3d_bytes")
    val fields = Seq(
      col(DERIVED_DIMS),
      col(DERIVED_DATE),
      col(DERIVED_UNIX_TIME),
      setCount3d,
      setSerde3d
    )
    val result = df.select(fields: _*).dropDuplicates(DERIVED_DIMS, DERIVED_DATE)
    result.show(100)
  }
}
