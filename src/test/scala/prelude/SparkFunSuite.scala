package prelude

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

/**
 * Basic harness for running a local spark executor without installing spark binary.
 *
 * @note To run tests on windows, download <a href="https://github.com/steveloughran/winutils">winutils</a>
 *       and add the __bin__ folder to system environment variable lists, then set system property `hadoop.home.dir` to
 *       the hadoop folder(not bin folder).
 */
abstract class SparkFunSuite extends AnyFunSuite with BeforeAndAfterAll {

  var sparkSession: SparkSession = _

  override protected def beforeAll(): Unit = {
    sparkSession = SparkSession
      .builder()
      .appName("SparkFunSuite")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .master("local[*]")
      .getOrCreate()
    println(s"A ${sparkSession.sparkContext.getClass.getCanonicalName} was created")
  }
}
