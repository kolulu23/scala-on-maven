package metric

import metric.MetricInvokeRecordAnalysis._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import prelude.SparkFunSuite

/**
 *
 */
class MetricInvokeRecordAnalysis extends SparkFunSuite {
  var df: DataFrame = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    sparkSession.udf.register(
      "entropy",
      udf((arr: Array[Double], total: Double) => MetricInvokeRecordAnalysis.entropy(arr, total))
    )
    sparkSession.udf.register(
      "gini_impurity",
      udf((arr: Array[Double], total: Double) => MetricInvokeRecordAnalysis.giniImpurity(arr, total))
    )
    df = sparkSession
      .read
      .option("header", value = true)
      .schema(MIN_DATA_SCHEMA)
      .csv(s"${MetricInvokeRecordAnalysis.MIH_DATA_PATH}/small.csv")
    df.show()
  }

  test("two-label-agg") {
    val groupedDf = df.groupBy(MIN_DATA_FIELD_METRIC_CODE.name)
      .agg(
        count("*").as("total"),
        array(
          count_if(col(MIN_DATA_FIELD_LABEL.name) === lit(1)),
          count_if(col(MIN_DATA_FIELD_LABEL.name) === lit(0))
        ).as("label_count")
      ).select(
        col(MIN_DATA_FIELD_METRIC_CODE.name),
        expr("entropy(label_count, total)").as("entropy_root"),
        expr("gini_impurity(label_count, total)").as("gini_impurity_root")
      )
    groupedDf.show()
  }

  /**
   * Note: Hardcoded buckets is essentially just group items evenly without
   * even look into their value.
   * Info gain and gini impurity calculated in this way is pretty much useless.
   */
  test("partition-and-hardcoded-buckets") {
    val bucketId = "bucket_id"
    val bucketNum = 2
    // Root entropy is calculated directly on label column
    val rootEntropyDf = df.groupBy(MIN_DATA_FIELD_METRIC_CODE.name)
      .agg(
        count("*").as("total"),
        array(
          count_if(col(MIN_DATA_FIELD_LABEL.name) === lit(1)),
          count_if(col(MIN_DATA_FIELD_LABEL.name) === lit(0))
        ).as("label_count")
      ).select(
        col(MIN_DATA_FIELD_METRIC_CODE.name),
        col("total"),
        expr("entropy(label_count, total)").as("entropy_root")
      )
    // Metric(feature) entropy and gini is calculated with hardcoded buckets
    val window = Window
      .partitionBy(col(MIN_DATA_FIELD_METRIC_CODE.name))
      .orderBy(col(MIN_DATA_FIELD_METRIC_RESULT.name))
    val bucketsDf = df.withColumn(bucketId, ntile(bucketNum) over window)
    bucketsDf.show()

    val pivotedDf = bucketsDf.groupBy(MIN_DATA_FIELD_METRIC_CODE.name).pivot(col(bucketId))
      .agg(count("*"))
    pivotedDf.show()

    val bucketIdCols = pivotedDf.schema
      .filterNot(p => p.name == MIN_DATA_FIELD_METRIC_CODE.name)
      .map(p => ifnull(col(p.name), lit(0)))
    pivotedDf
      .join(rootEntropyDf, MIN_DATA_FIELD_METRIC_CODE.name, "left")
      .withColumn("label_count", array(bucketIdCols: _*))
      .select(
        col(MIN_DATA_FIELD_METRIC_CODE.name),
        expr("entropy(label_count, total)").as("entropy"),
        expr("gini_impurity(label_count, total)").as("gini_impurity"),
        col("entropy_root"),
        (col("entropy_root") - col("entropy")).as("info_gain")
      )
      .show()
  }

  test("using-mllib-discrete") {
    val df2 = df
      .filter(col(MIN_DATA_FIELD_VALUE_DATA_TYPE.name) !== lit("STRING"))
      .withColumn("num_result", col(MIN_DATA_FIELD_METRIC_RESULT.name).cast(DataTypes.DoubleType))
      .groupBy(col(MIN_DATA_FIELD_BIZ_ID.name))
      .pivot(col(MIN_DATA_FIELD_METRIC_CODE.name))
      .agg(
        collect_list(col("num_result"))
      )
    df2.show()
  }
}

object MetricInvokeRecordAnalysis {
  val MIH_DATA_PATH = "src/test/resources/data/metric_invoke_history"

  val MIN_DATA_FIELD_BIZ_ID: StructField = StructField("BIZ_ID", DataTypes.StringType)
  val MIN_DATA_FIELD_BIZ_TIME: StructField = StructField("BIZ_TIME", DataTypes.LongType)
  val MIN_DATA_FIELD_METRIC_CODE: StructField = StructField("METRIC_CODE", DataTypes.StringType)
  val MIN_DATA_FIELD_METRIC_RESULT: StructField = StructField("METRIC_RESULT", DataTypes.StringType)
  val MIN_DATA_FIELD_CALLER: StructField = StructField("CALLER", DataTypes.StringType)
  val MIN_DATA_FIELD_DS: StructField = StructField("DS", DataTypes.DateType)
  val MIN_DATA_FIELD_DEFAULT_VALUE: StructField = StructField("DEFAULT_VALUE", DataTypes.StringType)
  val MIN_DATA_FIELD_VALUE_DATA_TYPE: StructField = StructField("VALUE_DATA_TYPE", DataTypes.StringType)
  val MIN_DATA_FIELD_LABEL: StructField = StructField("LABEL", DataTypes.IntegerType)

  val MIN_DATA_SCHEMA: StructType = StructType(Seq(
    MIN_DATA_FIELD_BIZ_ID,
    MIN_DATA_FIELD_BIZ_TIME,
    MIN_DATA_FIELD_METRIC_CODE,
    MIN_DATA_FIELD_METRIC_RESULT,
    MIN_DATA_FIELD_CALLER,
    MIN_DATA_FIELD_DS,
    MIN_DATA_FIELD_DEFAULT_VALUE,
    MIN_DATA_FIELD_VALUE_DATA_TYPE,
    MIN_DATA_FIELD_LABEL
  ))

  /**
   * See also [[org.apache.spark.mllib.tree.impurity.Entropy]]
   *
   * @param arr   Label count array, each element is the number of that label
   * @param total Total samples
   * @return Entropy
   */
  def entropy(arr: Array[Double], total: Double): Double = {
    if (total == 0) {
      return 0D
    }
    val numLabels = arr.length
    var impurity = 0D
    var labelIdx = 0
    while (labelIdx < numLabels) {
      val labelCnt = arr(labelIdx)
      if (labelCnt != 0) {
        val freq = labelCnt / total
        impurity -= freq * (math.log(freq) / math.log(2))
      }
      labelIdx += 1
    }
    impurity
  }

  /**
   * See also [[org.apache.spark.mllib.tree.impurity.Gini]]
   *
   * @param arr   Label count array, each element is the number of that label
   * @param total Total samples
   * @return Gini impurity
   */
  def giniImpurity(arr: Array[Double], total: Double): Double = {
    if (total == 0) {
      return 0D
    }
    val numLabels = arr.length
    var impurity = 1D
    var labelIdx = 0
    while (labelIdx < numLabels) {
      val freq = arr(labelIdx) / total
      impurity -= freq * freq
      labelIdx += 1
    }
    impurity
  }
}