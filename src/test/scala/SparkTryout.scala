import FakeSomeData.TRANS_DATA_PATH
import SparkTryout.{DERIVED_DATE, DERIVED_DIMS}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
 * Does some prototyping on spark's groupBy+aggregator+window api.
 *
 * To run this test, first you need generate some fake data with [[FakeSomeData]].
 * Then if you are on windows, you should download <a href="https://github.com/steveloughran/winutils">winutils</a>
 * and add the __bin__ folder to your system environment variable lists.
 */
class SparkTryout extends SparkFunSuite {

  var df: DataFrame = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    df = sparkSession
      .read
      .option("header", value = true)
      .csv(TRANS_DATA_PATH)
      .withColumn(DERIVED_DIMS, concat_ws("#", col("dim"), col("sub_dim")))
      .withColumn(DERIVED_DATE, from_unixtime(col("biz_time") / 1000, "yyyy-MM-dd"))
  }

  test("GroupBy on DataFrame Test") {
    val groupedDf = df
      .groupBy(DERIVED_DIMS)
      .pivot(DERIVED_DATE)
      .agg(
        count(DERIVED_DIMS),
        sum("trans_amount")
      )
    groupedDf.show()
    println(s"Total: ${groupedDf.count()}")
  }

  test("GroupBy and Windowing") {
    val windowSpec = Window
      .partitionBy(col(DERIVED_DIMS))
      .orderBy(unix_timestamp(col(DERIVED_DATE), "yyyy-MM-dd"))
    val last3days = windowSpec.rangeBetween(-3 * 24 * 60 * 60, Window.currentRow)
    val last5days = windowSpec.rangeBetween(-5 * 24 * 60 * 60, Window.currentRow)
    df.select(
      col(DERIVED_DIMS),
      col(DERIVED_DATE),
      (count(DERIVED_DIMS) over last3days).as("cnt3"),
      (count(DERIVED_DIMS) over last5days).as("cnt5"),
      (sum("trans_amount") over last3days).as("sum3")
    ).dropDuplicates(DERIVED_DIMS, DERIVED_DATE)
      .show()
  }

}

object SparkTryout {
  val DERIVED_DIMS = "union_dim"
  val DERIVED_DATE = "derived_date"
}
