import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import java.time.{LocalDateTime, ZoneOffset}
import scala.util.Random

class SparkTryout extends AnyFunSuite with BeforeAndAfterAll with BeforeAndAfterEach {

  var sparkSession: SparkSession = _

  val rand = new Random(736132)

  val DATA_PATH = "src/test/resources/fake_transaction_data"

  override protected def beforeAll(): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\Users\\Lu\\lib_deps\\hadoop-2.8.1")
    sparkSession = SparkSession
      .builder()
      .appName("SparkTryout")
      .config("spark.some.config.option", "some-value")
      .master("local[*]")
      .getOrCreate()
    println(s"A ${sparkSession.sparkContext.getClass.getCanonicalName} was created")
  }

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
    val allSubDims = (0 to 12).map(_ => rand.nextInt(99))
    val allStatus = Seq(0, 0, 1, 1, 2, 3, 0, 0)
    val allIds = (0 to 512).map(_ => List.fill(16)(rand.nextPrintableChar()).mkString)
    var bizTime = LocalDateTime
      .of(2022, 11, 26, 16, 7, 52)
      .toInstant(ZoneOffset.UTC)
      .toEpochMilli
    val rows = (0 to 10000).map(_ => {
      val srcIndex = rand.nextInt(allIds.size)
      val targetIndex = (srcIndex + 16).min(511)
      bizTime += (rand.nextGaussian() * 86400D).longValue()
      Row.fromSeq(Seq(
        allDims(rand.nextInt(allDims.size)),
        allSubDims(rand.nextInt(allSubDims.size)),
        bizTime,
        rand.nextGaussian() * 123422D,
        allIds(srcIndex),
        allIds(targetIndex),
        allStatus(rand.nextInt(allStatus.size))
      ))
    })
    val df = sparkSession.createDataFrame(sparkSession.sparkContext.parallelize(rows), schema)
    df.show(truncate = false)
    df.coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .option("header", value = true)
      .csv(DATA_PATH)
  }
}
