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
    val (colBucketId, colMetricCode, colLabel, colMetricResult) = (
      col("bucket_id"),
      col(MIN_DATA_FIELD_METRIC_CODE.name),
      col(MIN_DATA_FIELD_LABEL.name),
      col(MIN_DATA_FIELD_METRIC_RESULT.name)
    )
    val bucketNum = 3
    // Root entropy is calculated directly on label column
    val rootEntropyDf = df.groupBy(colMetricCode)
      .agg(
        count("*").as("total"),
        array(
          count_if(colLabel === lit(1)),
          count_if(colLabel === lit(0))
        ).as("label_count")
      ).select(
        colMetricCode,
        col("total"),
        expr("entropy(label_count, total)").as("entropy_root"),
        // Here's something special, we solely replies on the label input instead of using buckets
        // This is partially because manually split buckets is useless for the semantics of gini impurity
        expr("gini_impurity(label_count, total)").as("gini_impurity")
      )
    // Metric(feature) entropy is calculated with hardcoded buckets
    val window = Window.partitionBy(colMetricCode).orderBy(colMetricResult)
    val bucketsDf = df.withColumn("bucket_id", ntile(bucketNum) over window)
    bucketsDf.show()

    val pivotedDf = bucketsDf
      .groupBy(colMetricCode, colBucketId)
      .pivot(colLabel)
      .agg(count("*"))
    val labelIdCols = Seq(ifnull(col("1"), lit(0)), ifnull(col("0"), lit(0)))
    // To calculate split information and eventually information gain
    val pivotedPerBucketDf = bucketsDf
      .groupBy(colMetricCode)
      .pivot(colBucketId)
      .agg(count("*"))
    val bucketIdCols = pivotedPerBucketDf.schema
      .filterNot(p => p.name == MIN_DATA_FIELD_METRIC_CODE.name)
      .map(p => ifnull(col(p.name), lit(0)))
    val splitInfoDf = pivotedPerBucketDf
      .join(rootEntropyDf, MIN_DATA_FIELD_METRIC_CODE.name, "left")
      .withColumn("bucket_size_count", array(bucketIdCols: _*))
      .select(
        colMetricCode,
        expr("entropy(bucket_size_count, total)").as("split_info"),
        col("gini_impurity")
      )
    pivotedDf
      .join(rootEntropyDf, MIN_DATA_FIELD_METRIC_CODE.name, "left")
      .select(
        colMetricCode,
        col("entropy_root"),
        array(labelIdCols: _*).as("label_count"),
        aggregate(
          col("label_count"),
          lit(0).cast(DataTypes.LongType),
          (acc, x) => acc + x.cast(DataTypes.LongType)
        ).as("total_per_bucket"),
        (expr("entropy(label_count, total)") * col("total_per_bucket") / col("total")).as("entropy_weighted")
      )
      .groupBy(colMetricCode)
      .agg(
        (first("entropy_root") - sum("entropy_weighted")).as("info_gain")
      )
      .join(splitInfoDf, MIN_DATA_FIELD_METRIC_CODE.name, "left")
      .withColumn("ingo_gain_ratio", col("info_gain") / col("split_info"))
      .explain(true)
  }

  test("using-mllib-discrete") {
    ???
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