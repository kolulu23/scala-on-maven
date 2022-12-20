import FakeSomeData.TRANS_DATA_PATH
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode}

import java.time.{LocalDateTime, ZoneOffset}
import scala.util.Random

/**
 * Fake transaction-like event data.
 * Some characteristics should be noted:
 *  i. Transaction amount is gaussian distributed
 *  i. It also mimics keyed data(`dim` and `sub_dim`)
 *  i. Data skew is not phenomenal, but you can tweak this by adding huge anomaly
 *  i. Data sink is just compressed csv with default partitions as it is portable enough to be dealt with
 *  i. Both `trans_source_id` and `trans_target_id`'s length is fixed, and they are almost evenly distributed
 *  i. `trans_status` is not evenly distributed
 *  i. Cardinality matters, the total number of `trans_source_id` should be at least three times larger than the
 *      number of `dim` and `sub_dim` combinations. It does look weired though.
 *
 */
class FakeSomeData extends SparkFunSuite {

  val rand = new Random(556887221)

  test("Make up some data, runs only once") {
    val schema = StructType(Seq(
      StructField("dim", DataTypes.StringType),
      StructField("sub_dim", DataTypes.IntegerType),
      StructField("biz_time", DataTypes.LongType),
      StructField("trans_amount", DataTypes.DoubleType),
      StructField("trans_source_id", DataTypes.StringType),
      StructField("trans_target_id", DataTypes.StringType),
      StructField("trans_status", DataTypes.IntegerType)
    ))
    val allDims = Seq("John", "Mike", "Michale", "Hill")
    val allSubDims = (0 to 24).map(_ => rand.nextInt(99))
    val allStatus = Seq(0, 0, 1, 1, 2, 3, 0, 0)
    val allIds = (0 to 512).map(_ => List.fill(16)(rand.nextPrintableChar()).mkString)
    var bizTime = LocalDateTime
      .of(2022, 11, 26, 16, 7, 52)
      .toInstant(ZoneOffset.UTC)
      .toEpochMilli
    // Fake 500000 rows of transaction data from 2022-11-16 to 2022-11-27
    val rows = (1 to 500000).map(_ => {
      val srcIndex = rand.nextInt(allIds.size)
      val targetIndex = (srcIndex + 16).min(511)
      bizTime += (rand.nextGaussian() * 86400 * 5).longValue()
      Row.fromSeq(Seq(
        allDims(rand.nextInt(allDims.size)),
        allSubDims(rand.nextInt(allSubDims.size)),
        bizTime,
        ((rand.nextGaussian() + 1D) * 3000D).abs,
        allIds(srcIndex),
        allIds(targetIndex),
        allStatus(rand.nextInt(allStatus.size))
      ))
    }).toBuffer
    // Add some anomaly data
    rows.append(Row.fromSeq(Seq("Lulu", 22, bizTime + 86400 * 2, 100D, "Me", "You", 0)))
    rows.append(Row.fromSeq(Seq("Lulu", 22, bizTime + 86400 * 2, 900D, "You", "Me", 1)))
    // Data is evenly distributed around "dim" and "sub_dim"
    val df = sparkSession.createDataFrame(sparkSession.sparkContext.parallelize(rows), schema)
    df.show(truncate = false)
    df.describe("trans_amount").show(truncate = false)
    df.write
      .mode(SaveMode.Overwrite)
      .option("header", value = true)
      .csv(TRANS_DATA_PATH)
  }
}

object FakeSomeData {
  val TRANS_DATA_PATH = "src/test/resources/data/transactions"
}
