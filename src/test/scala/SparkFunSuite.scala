import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

abstract class SparkFunSuite extends AnyFunSuite with BeforeAndAfterAll {

  var sparkSession: SparkSession = _

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
}
