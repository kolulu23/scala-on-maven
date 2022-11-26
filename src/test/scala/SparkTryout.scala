import FakeSomeData.TRANS_DATA_PATH
import org.apache.spark.sql.functions._

class SparkTryout extends SparkFunSuite {

  test("GroupBy on DataFrame Test") {
    val df = sparkSession
      .read
      .option("header", value = true)
      .csv(TRANS_DATA_PATH)
      .withColumn("union_dim", concat_ws("#", col("dim"), col("sub_dim")))
      .withColumn("derived_date", from_unixtime(col("biz_time") / 1000, "yyyy-MM-dd"))
    val groupedDf = df
      .groupBy("union_dim")
      .pivot("derived_date")
      .agg(
        count("union_dim"),
        sum("trans_amount")
      )
    groupedDf.show()
    println(s"Total: ${groupedDf.count()}")
  }
}
