import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite

class SparkTryout extends AnyFunSuite with BeforeAndAfterAll with BeforeAndAfterEach {

  var sparkSession: SparkSession = _

  override protected def beforeAll(): Unit = {
    sparkSession = SparkSession
      .builder()
      .appName("SparkTryout")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    println(s"A ${sparkSession.sparkContext.getClass.getCanonicalName} was created")
  }

  test("Spark Creation") {

  }
}
